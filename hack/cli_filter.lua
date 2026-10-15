-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- cli_filter.lua - Slurm client-side CLI filter plugin for k8s-bridge flag validation.
-- Enforces k8s-bridge CLI option validation on workload-mixing partitions.
-- Unsupported options are rejected prior to slurmctld submission.

local REJECTION_MESSAGE = "Validation error: job uses an option that is not supported by k8s-bridge"

-- Allowlist of CLI option names permitted on k8s-bridge managed partitions.
local ALLOWED_OPTIONS = {
    -- Special key emitted by slurm.json_cli_options for positional command arguments
    ["argv"] = true,

    -- Container options
    ["container"]      = true, -- --container
    ["container-id"]   = true, -- --container-id
    ["container-type"] = true, -- --container-type

    -- Slurm step and job execution options
    ["deadline"]            = true, -- --deadline
    ["extra"]               = true, -- --extra
    ["gid"]                 = true, -- --gid
    ["hint"]                = true, -- --hint
    ["relative"]            = true, -- -r, --relative
    ["signal"]              = true, -- --signal
    ["time"]                = true, -- -t, --time
    ["kill-on-invalid-dep"] = true, -- --kill-on-invalid-dep
    ["overcommit"]          = true, -- -O, --overcommit
    ["propagate"]           = true, -- --propagate
    ["stepmgr"]             = true, -- --stepmgr
    ["send-libs"]           = true, -- --send-libs
    ["profile"]             = true, -- --profile
    ["pty"]                 = true, -- --pty
    ["reservation"]         = true, -- --reservation
    ["resources"]           = true, -- --resources
    ["slurmd-debug"]        = true, -- --slurmd-debug
    ["tmp"]                 = true, -- --tmp
    ["bcast"]               = true, -- --bcast
    ["bcast-exclude"]       = true, -- --bcast-exclude
    ["chdir"]               = true, -- -D, --chdir
    ["distribution"]        = true, -- -m, --distribution
    ["exact"]               = true, -- --exact
    ["external-launcher"]   = true, -- --external-launcher
    ["gpu-bind"]            = true, -- --gpu-bind
    ["kill-on-bad-exit"]    = true, -- -K, --kill-on-bad-exit
    ["label"]               = true, -- -l, --label
    ["multi-prog"]          = true, -- --multi-prog
    ["oom-kill-step"]       = true, -- --oom-kill-step
    ["output"]              = true, -- -o, --output
    ["overlap"]             = true, -- --overlap
    ["preserve-env"]        = true, -- -E, --preserve-env
    ["uid"]                 = true, -- --uid
    ["unbuffered"]          = true, -- -u, --unbuffered
    ["wait-for-children"]   = true, -- --wait-for-children
    ["wckey"]               = true, -- --wckey
    ["acctg-freq"]          = true, -- --acctg-freq
    ["account"]             = true, -- -A, --account
    ["bb"]                  = true, -- --bb
    ["bbf"]                 = true, -- --bbf
    ["cluster-constraint"]  = true, -- --cluster-constraint
    ["clusters"]            = true, -- -M, --clusters
    ["compress"]            = true, -- --compress
    ["error"]               = true, -- -e, --error
    ["input"]               = true, -- -i, --input
    ["licenses"]            = true, -- -L, --licenses
    ["mail-type"]           = true, -- --mail-type
    ["mail-user"]           = true, -- --mail-user
    ["task-epilog"]         = true, -- --task-epilog
    ["task-prolog"]         = true, -- --task-prolog

    -- Resource allocation and CLI control options
    ["tres-per-task"]     = true, -- --tres-per-task
    ["mpi"]               = true, -- --mpi
    ["async"]             = true, -- --async
    ["batch"]             = true, -- --batch
    ["bell"]              = true, -- --bell
    ["comment"]           = true, -- --comment
    ["cpus-per-gpu"]      = true, -- --cpus-per-gpu
    ["cpus-per-task"]     = true, -- -c, --cpus-per-task
    ["disable-status"]    = true, -- -X, --disable-status
    ["epilog"]            = true, -- --epilog
    ["export"]            = true, -- --export
    ["export-file"]       = true, -- --export-file
    ["get-user-env"]      = true, -- --get-user-env
    ["gpus"]              = true, -- -G, --gpus
    ["gpus-per-node"]     = true, -- --gpus-per-node
    ["gpus-per-task"]     = true, -- --gpus-per-task
    ["gres"]              = true, -- --gres
    ["help"]              = true, -- -h, --help
    ["hold"]              = true, -- -H, --hold
    ["ignore-pbs"]        = true, -- --ignore-pbs
    ["ignore-signals"]    = true, -- --ignore-signals
    ["job-name"]          = true, -- -J, --job-name
    ["kill-command"]      = true, -- -K, --kill-command
    ["mem"]               = true, -- --mem
    ["mem-per-cpu"]       = true, -- --mem-per-cpu
    ["mem-per-gpu"]       = true, -- --mem-per-gpu
    ["msg-timeout"]       = true, -- --msg-timeout
    ["no-bell"]           = true, -- --no-bell
    -- --no-requeue marks the job as not requeueable (requeue=0). k8s-bridge translates it
    -- to a JobSet that is not restarted after a failure (failurePolicy.maxRestarts=0).
    -- --requeue stays rejected: JobRequeue=1 is set cluster-side instead.
    ["no-requeue"]        = true, -- --no-requeue
    ["no-shell"]          = true, -- --no-shell
    ["nodes"]             = true, -- -N, --nodes
    ["ntasks"]            = true, -- -n, --ntasks
    ["ntasks-per-node"]   = true, -- --ntasks-per-node
    ["tasks-per-node"]    = true, -- alias for --ntasks-per-node
    ["open-mode"]         = true, -- --open-mode
    ["parsable"]          = true, -- --parsable
    ["partition"]         = true, -- -p, --partition
    ["prolog"]            = true, -- --prolog
    ["quiet"]             = true, -- -Q, --quiet
    ["quit-on-interrupt"] = true, -- --quit-on-interrupt
    ["test-only"]         = true, -- --test-only
    ["threads"]           = true, -- -T, --threads
    ["treewidth"]         = true, -- --treewidth
    ["usage"]             = true, -- --usage
    ["verbose"]           = true, -- -v, --verbose
    ["version"]           = true, -- -V, --version
    ["wait"]              = true, -- -W, --wait
    ["wait-all-nodes"]    = true, -- --wait-all-nodes
    ["wrap"]              = true, -- --wrap
}

-- Helper: Split a comma-separated list into trimmed, non-empty entries.
local function split_csv(val)
    local list = {}
    if not val or val == "unset" then
        return list
    end
    for part in string.gmatch(val, "[^,]+") do
        part = part:match("^%s*(.-)%s*$")
        if part ~= "" then
            table.insert(list, part)
        end
    end
    return list
end

-- Helper: Parse comma-separated string into a lookup table of partition names.
-- Ignores other key=value parameters (e.g. cli_filter_lua_path=...) if present in CliFilterParameters.
local function parse_wm_partitions(val)
    if not val or val:match("^%s*$") then
        return nil
    end
    local map = {}
    for _, part in ipairs(split_csv(val)) do
        if not part:find("=", 1, true) then
            map[part] = true
        end
    end
    return map
end

-- Helper: Read slurm.conf content with comments stripped and backslash continuations joined.
local function read_clean_slurm_conf()
    local conf_path = os.getenv("SLURM_CONF") or "/etc/slurm/slurm.conf"
    local f = io.open(conf_path, "r")
    if not f then
        return nil
    end
    local content = f:read("*a")
    f:close()
    if not content then
        return nil
    end
    return content:gsub("#[^\n]*", ""):gsub("\\[ \t]*\r?\n", " ")
end

-- Helper: Read workload_mixing_partitions from CliFilterParameters in slurm.conf.
local function get_wm_partitions_from_conf()
    local content = read_clean_slurm_conf()
    if not content then
        return nil
    end
    for line in content:gmatch("[^\r\n]+") do
        local lower_line = line:lower()
        if lower_line:match("^%s*clifilterparameters%s*=") then
            local _, val_start = lower_line:find("workload_mixing_partitions%s*=%s*")
            if val_start then
                local wm = line:sub(val_start + 1):match("^([^;#\r\n]+)")
                if wm then
                    wm = wm:gsub("[\"']", ""):match("^%s*(.-)%s*$")
                    if wm ~= "" then
                        return wm
                    end
                end
            end
        end
    end
    return nil
end

-- Helper: Read default partition from slurm.conf if no partition was explicitly specified.
local function get_default_partition_from_conf()
    local content = read_clean_slurm_conf()
    if not content then
        return nil
    end
    for line in content:gmatch("[^\r\n]+") do
        local lower_line = line:lower()
        local _, name_start = lower_line:find("partitionname%s*=%s*")
        if name_start and lower_line:match("default%s*=%s*yes") then
            local part_name = line:sub(name_start + 1):match("^([^%s]+)")
            if part_name then
                return part_name
            end
        end
    end
    return nil
end

-- Cache slurm.conf lookups at module initialization
local wm_map = parse_wm_partitions(get_wm_partitions_from_conf())
local cached_default_partition = get_default_partition_from_conf()

-- Helper: Extract top-level option names from the flat JSON produced by slurm.json_cli_options.
-- Strips escaped characters (\" and \\) and neutralizes string/array values so only keys match.
local function parse_json_top_level_keys(json_str)
    local keys = {}
    if type(json_str) ~= "string" then
        return keys
    end
    local clean = json_str:gsub("\\.", ""):gsub(':%s*"[^"]*"', ":null"):gsub(":%s*%b[]", ":null")
    for k in clean:gmatch('"([^"]+)"%s*:') do
        keys[k] = true
    end
    return keys
end

-- Core validation logic executed during slurm_cli_pre_submit.
local function pre_submit_validate(options, pack_offset)
    local current_wm_map = wm_map or parse_wm_partitions(get_wm_partitions_from_conf())
    if not current_wm_map then
        return slurm.SUCCESS
    end

    local active_opts = {}
    if slurm and type(slurm.json_cli_options) == "function" and options then
        local ok, json_str = pcall(slurm.json_cli_options, options)
        if ok and type(json_str) == "string" and json_str ~= "" then
            active_opts = parse_json_top_level_keys(json_str)
        end
    end

    -- Check options["het-group"] explicitly as Slurm's json_cli_options omits het-group
    if options then
        local ok_hg, hg_val = pcall(function() return options["het-group"] end)
        if ok_hg and hg_val ~= nil then
            local hg_str = tostring(hg_val)
            if hg_str ~= "" and hg_str ~= "unset" and hg_str ~= "invalid-context" then
                active_opts["het-group"] = true
            end
        end
    end

    -- Determine requested partition(s)
    local partition_str = nil
    if options then
        local ok, val = pcall(function() return options["partition"] end)
        if ok and val ~= nil then
            local val_str = tostring(val)
            if val_str ~= "" and val_str ~= "unset" then
                partition_str = val_str
            end
        end
    end
    if not partition_str or partition_str == "" or partition_str == "unset" then
        partition_str = cached_default_partition or get_default_partition_from_conf()
    end

    local target_parts = split_csv(partition_str)
    local is_wm = false
    for _, part in ipairs(target_parts) do
        if current_wm_map[part] then
            is_wm = true
            break
        end
    end

    if not is_wm then
        return slurm.SUCCESS
    end

    -- Reject heterogeneous job components (pack_offset > 0)
    if type(pack_offset) == "number" and pack_offset > 0 then
        slurm.log_error(string.format("%s (heterogeneous job pack_offset=%d)", REJECTION_MESSAGE, pack_offset))
        return slurm.ERROR
    end

    -- Check all active CLI options against the allowlist
    for opt_name, _ in pairs(active_opts) do
        local opt_str = tostring(opt_name)
        if not ALLOWED_OPTIONS[opt_str] and not opt_str:match("^spank:") then
            slurm.log_error(string.format("%s (--%s)", REJECTION_MESSAGE, opt_str))
            return slurm.ERROR
        end
    end

    return slurm.SUCCESS
end

-- Required Hook 1: slurm_cli_setup_defaults
function slurm_cli_setup_defaults(options, early)
    return slurm.SUCCESS
end

-- Required Hook 2: slurm_cli_pre_submit
function slurm_cli_pre_submit(options, pack_offset)
    return pre_submit_validate(options, pack_offset)
end

-- Required Hook 3: slurm_cli_post_submit
function slurm_cli_post_submit(offset, jobid, stepid)
    return slurm.SUCCESS
end
