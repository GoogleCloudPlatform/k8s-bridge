VERSION ?= $(shell tr -d '[:space:]' < ./VERSION)
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
LDFLAGS ?= -X github.com/GoogleCloudPlatform/k8s-bridge/internal/version.Version=$(VERSION) \
	-X github.com/GoogleCloudPlatform/k8s-bridge/internal/version.GitCommit=$(GIT_COMMIT) \
	-X github.com/GoogleCloudPlatform/k8s-bridge/internal/version.BuildDate=$(BUILD_DATE)

# Image URL to use all building/pushing image targets
IMG ?= ghcr.io/googlecloudplatform/k8s-bridge:$(VERSION)
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

-include .env

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: version
version: ## Show current version.
	@echo $(VERSION)

.PHONY: version-sync
version-sync: ## Synchronize VERSION with Helm chart, Kustomize metadata, and documentation.
	@if [ -z "$$(echo $(VERSION) | grep -Eo "^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?$$")" ]; then \
		echo "Error: VERSION is not valid semver: $(VERSION)" >&2; \
		exit 1; \
	fi
	sed -E -e 's/^version:[[:space:]]+.+$$/version: $(VERSION)/' \
		-e 's/^appVersion:[[:space:]]+.+$$/appVersion: "$(VERSION)"/' \
		helm/Chart.yaml > helm/Chart.yaml.tmp && mv helm/Chart.yaml.tmp helm/Chart.yaml
	sed -E 's/^([[:space:]]*newTag:[[:space:]]+).+$$/\1$(VERSION)/' \
		config/manager/kustomization.yaml > config/manager/kustomization.yaml.tmp && \
		mv config/manager/kustomization.yaml.tmp config/manager/kustomization.yaml
	for f in README.md helm/README.md docs/quickstart.md docs/existing-infra.md; do \
		sed -E \
			-e '/helm .*oci:\/\/ghcr\.io\/googlecloudplatform\/charts\/k8s-bridge/{n;s/(--version[[:space:]]+)[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?/\1$(VERSION)/;}' \
			-e 's/^(\| k8s-bridge \| `oci:\/\/ghcr\.io\/googlecloudplatform\/charts\/k8s-bridge` \| )[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.-]+)?( \|)$$/\1$(VERSION)\3/' \
			"$$f" > "$$f.tmp" && mv "$$f.tmp" "$$f"; \
	done

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role,headerFile="hack/boilerplate.yaml.txt",year=$(YEAR) crd:generateEmbeddedObjectMeta=true,headerFile="hack/boilerplate.yaml.txt",year=$(YEAR) webhook:headerFile="hack/boilerplate.yaml.txt",year=$(YEAR) paths="./..." output:crd:artifacts:config=config/crd/bases
	cp config/crd/bases/k8s-bridge.x-k8s.io_workloadmixings.yaml helm/crds/k8s-bridge.x-k8s.io_workloadmixings.yaml

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test --race $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
# JobSet is installed by default; skip with:
# - JOBSET_INSTALL_SKIP=true
KIND_CLUSTER ?= k8s-bridge-test-e2e
KIND_NODE_IMAGE ?= kindest/node:v1.32.2
KIND_CONFIG ?= test/e2e/testdata/kind-config.yaml
# When KIND_KUBECONFIG is set, kind writes the cluster credentials to this file
# instead of ~/.kube/config, and the current context of ~/.kube/config stays as it is.
# The e2e tests then read the file through KUBECONFIG.
KIND_KUBECONFIG ?=
kind_kubeconfig_flag = $(if $(KIND_KUBECONFIG),--kubeconfig $(KIND_KUBECONFIG))

# The resilience e2e suite stops components while jobs run, so it runs on its own
# Kind cluster. Its cluster name must not contain KIND_CLUSTER.
KIND_CLUSTER_RESILIENCE ?= k8s-bridge-e2e-resilience
KIND_KUBECONFIG_RESILIENCE ?= $(LOCALBIN)/kubeconfig-$(KIND_CLUSTER_RESILIENCE)
RESILIENCE_TEST_TIMEOUT ?= 40m

.PHONY: setup-test-e2e
setup-test-e2e: kind ## Set up a Kind cluster for e2e tests if it does not exist
	@if $(KIND) get clusters 2>/dev/null | grep -qx "$(KIND_CLUSTER)"; then \
		echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation."; \
		$(if $(KIND_KUBECONFIG),$(KIND) export kubeconfig --name $(KIND_CLUSTER) $(kind_kubeconfig_flag);) \
	else \
		echo "Creating Kind cluster '$(KIND_CLUSTER)' with image $(KIND_NODE_IMAGE)..."; \
		$(KIND) create cluster --name $(KIND_CLUSTER) --image=$(KIND_NODE_IMAGE) --config=$(KIND_CONFIG) $(kind_kubeconfig_flag); \
	fi

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	$(if $(KIND_KUBECONFIG),KUBECONFIG=$(KIND_KUBECONFIG)) KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=e2e ./test/e2e/ -v -ginkgo.v -timeout 20m
	$(MAKE) cleanup-test-e2e

.PHONY: run-test-e2e-resilience
run-test-e2e-resilience: ## Run the resilience e2e tests against the existing resilience Kind cluster
	KUBECONFIG=$(KIND_KUBECONFIG_RESILIENCE) KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER_RESILIENCE) \
		go test -tags=e2e ./test/e2e/resilience/ -v -ginkgo.v -timeout $(RESILIENCE_TEST_TIMEOUT)

# The recipe calls setup-test-e2e and cleanup-test-e2e recursively instead of as
# prerequisites. With make -j2 test-e2e test-e2e-resilience, a shared prerequisite
# would run only once, with the variables of only one suite. The cleanup also runs
# when the suite fails, and the recipe then exits with the status of the suite.
.PHONY: test-e2e-resilience
test-e2e-resilience: manifests generate fmt vet ## Run the controller failure e2e tests on a dedicated Kind cluster.
	@status=0; \
	$(MAKE) setup-test-e2e KIND_CLUSTER=$(KIND_CLUSTER_RESILIENCE) KIND_KUBECONFIG=$(KIND_KUBECONFIG_RESILIENCE) || status=$$?; \
	if [ $$status -eq 0 ]; then \
		$(MAKE) run-test-e2e-resilience || status=$$?; \
	fi; \
	$(MAKE) cleanup-test-e2e KIND_CLUSTER=$(KIND_CLUSTER_RESILIENCE) KIND_KUBECONFIG=$(KIND_KUBECONFIG_RESILIENCE); \
	exit $$status

.PHONY: update-e2e-deps
update-e2e-deps: ## Vendor the pinned cert-manager/JobSet/Kueue manifests used by the e2e suites.
	hack/update-e2e-deps.sh

.PHONY: cleanup-test-e2e
cleanup-test-e2e: kind ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER) $(kind_kubeconfig_flag)

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ E2E GPU Testing

export INFRA_PROVIDER ?= gke
export GPU_CLUSTER_NAME ?= k8s-bridge-gpu-e2e
export GPU_TEST_TIMEOUT ?= 45m
export GCP_ZONE ?= us-central1-a
export GPU_NODE_COUNT ?= 1
export GPU_ACCELERATOR_TYPE ?= nvidia-tesla-t4
export GPU_ACCELERATOR_COUNT ?= 1

.PHONY: setup-infra-gpu
setup-infra-gpu: ## Set up GPU cluster infrastructure for e2e tests
	test/e2e/infra/gpu-cluster-setup.sh

.PHONY: cleanup-infra-gpu
cleanup-infra-gpu: ## Tear down GPU cluster infrastructure used for e2e tests
	test/e2e/infra/gpu-cluster-cleanup.sh

.PHONY: run-test-e2e-gpu
run-test-e2e-gpu: manifests generate fmt vet build ## Run the GPU e2e tests against the active cluster context
	go test -tags=e2e_gpu ./test/e2e/gpu/ -v -ginkgo.v -timeout $(GPU_TEST_TIMEOUT)

.PHONY: test-e2e-gpu
test-e2e-gpu: manifests generate fmt vet ## Run end-to-end GPU tests with automated setup and teardown
	@status=0; \
	$(MAKE) setup-infra-gpu || status=$$?; \
	if [ $$status -eq 0 ]; then \
		$(MAKE) run-test-e2e-gpu || status=$$?; \
	fi; \
	$(MAKE) cleanup-infra-gpu; \
	exit $$status

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -ldflags "$(LDFLAGS)" -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run -ldflags "$(LDFLAGS)" ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build --build-arg VERSION=$(VERSION) --build-arg GIT_COMMIT=$(GIT_COMMIT) --build-arg BUILD_DATE=$(BUILD_DATE) -t ${IMG} .
	@if [ "$${CI:-}" = "true" ]; then $(CONTAINER_TOOL) builder prune -af; fi

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	- $(CONTAINER_TOOL) buildx create --name k8s-bridge-builder
	$(CONTAINER_TOOL) buildx use k8s-bridge-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --build-arg VERSION=$(VERSION) --build-arg GIT_COMMIT=$(GIT_COMMIT) --build-arg BUILD_DATE=$(BUILD_DATE) --tag ${IMG} .
	- $(CONTAINER_TOOL) buildx rm k8s-bridge-builder

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply --server-side --force-conflicts -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply --server-side --force-conflicts -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: helm-install
helm-install: ## Install k8s-bridge to the K8s cluster via local Helm chart
	helm upgrade --install k8s-bridge ./helm --namespace k8s-bridge-system --create-namespace -f ./helm/values.yaml

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
LOCAL_KIND = $(LOCALBIN)/kind
KIND ?= $(LOCAL_KIND)
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.21.0
KIND_VERSION ?= v0.27.0

#ENVTEST_VERSION is the controller-runtime version to use for setup-envtest, derived from go.mod
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v")

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.12.2

.PHONY: kind
kind: $(LOCAL_KIND) ## Download kind locally if necessary.
$(LOCAL_KIND): $(LOCALBIN)
	$(call go-install-tool,$(LOCAL_KIND),sigs.k8s.io/kind,$(KIND_VERSION))

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
