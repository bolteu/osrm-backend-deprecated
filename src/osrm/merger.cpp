#include "osrm/merger.hpp"
#include "merger/merger.hpp"
#include "merger/merger_config.hpp"
#include "extractor/scripting_environment_lua.hpp"

namespace osrm
{
    void merge(const merger::MergerConfig merger_config) {
        extractor::Sol2ScriptingEnvironment scripting_environment_first(merger_config.profile_path_first.string());
        extractor::Sol2ScriptingEnvironment scripting_environment_second(merger_config.profile_path_second.string());

        merger::Merger(merger_config).run(scripting_environment_first, scripting_environment_second);
    }
}