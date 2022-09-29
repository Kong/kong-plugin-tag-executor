local kong = kong
local utils = require "kong.tools.utils"
local strfmt = string.format

local TagExecutor = {
  PRIORITY = 100005,
  VERSION = "1.0",
  NAME = "tag-executor"
}

--[[
  Only the phases that can be configured on non-global plugins are triggered
  by this plugin, so `certificate` and `rewrite` were omitted. This makes
  sense based on the plugin design; might it be worth including the `ws_`
  phases too? Otherwise, if this is just intended for some specific plugins
  then perhaps only those should be made configurable (e.g. plugins that define
  "invalid" phases could be disallowed, etc.).
]]
local PHASES = {
  rewrite       = "rewrite",
  access        = "access",
  header_filter = "header_filter",
  body_filter   = "body_filter",
  log           = "log",
  preread       = "preread",
}

local function find_first(items, predicate)
  for _, item in ipairs(items) do
    if predicate(item) then
      return item
    end
  end
end

local function filter_array_items(items, predicate)
  local out = {}

  for _, item in ipairs(items) do
    if predicate(item) then out[#out + 1] = item end
  end

  return out
end

local function build_plugin_name_predicate(target_name)

  return function(plugin_conf)
    return plugin_conf.name == target_name
  end

end

local function build_phase_predicate(phase)

  return function(plugin)
    local phase_fn = plugin.handler[phase]
    return phase_fn and type(phase_fn) == "function"
  end

end

--[[
  Rather than computing which functions (phases) to execute, for every phase of
  the tag-executor, for every request, depending on the route that was hit,
  could it be better to do that once for each `route & phase` and then memoize
  the invocation? This could be relevant for large configurations (many steps or
  plugins to iterate through).
]]
local function invoke_plugins_for_phase(phase, plugins_by_phase, steps, route)
  local phase_plugins = plugins_by_phase[phase]

  for _, step in ipairs(steps) do

    if not step.target_tag or utils.table_contains(route.tags, step.target_tag) then
      kong.log.debug(strfmt("Invoking '%s' tag execution '%s' on route '%s'", phase, step.name, route.name))

      for _, plugin in ipairs(phase_plugins) do

        local plugin_options = find_first(step.plugins, build_plugin_name_predicate(plugin.name))

        if plugin_options then
          kong.log.debug(strfmt("Invoking '%s' phase of plugin '%s'", phase, plugin.name))
          plugin.handler[phase](plugin.handler, plugin_options.config)
        end

      end

    else
      kong.log.debug(strfmt("Skipping '%s' tag execution '%s' on route '%s'", phase, step.name, route.name))
    end

  end

end

function TagExecutor:init_worker()
  local all_plugins = kong.db.plugins:get_handlers()

  local plugin_name = self.NAME

  local exclude_self = function(plugin)
    return plugin.name ~= plugin_name
  end

  local target_plugins = filter_array_items(all_plugins, exclude_self)

  self.plugins_by_phase = {}

  for _, phase in pairs(PHASES) do
    self.plugins_by_phase[phase] = filter_array_items(target_plugins, build_phase_predicate(phase))
  end

end

function TagExecutor:preread(config)
  invoke_plugins_for_phase(PHASES.preread, self.plugins_by_phase, config.tag_execute_steps, kong.router.get_route())
end

function TagExecutor:access(config)
  invoke_plugins_for_phase(PHASES.access, self.plugins_by_phase, config.tag_execute_steps, kong.router.get_route())
end

--[[
  plugins that define `:response()` cannot currently be configured due to the
  choice of implementing `:header_filter()` and `:body_filter()`,
  (https://docs.konghq.com/gateway/latest/plugin-development/custom-logic/#available-contexts).
  This is ok but maybe worth documenting. Perhaps, as mentioned above, such
  plugins should not be made configurable either.
]]
function TagExecutor:header_filter(config)
  invoke_plugins_for_phase(PHASES.header_filter, self.plugins_by_phase, config.tag_execute_steps, kong.router.get_route())
end

function TagExecutor:body_filter(config)
  invoke_plugins_for_phase(PHASES.body_filter, self.plugins_by_phase, config.tag_execute_steps, kong.router.get_route())
end

function TagExecutor:log(config)
  invoke_plugins_for_phase(PHASES.log, self.plugins_by_phase, config.tag_execute_steps, kong.router.get_route())
end

return TagExecutor
