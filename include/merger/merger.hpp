#include "extractor/extraction_containers.hpp"
#include "extractor/extractor_callbacks.hpp"
#include "extractor/maneuver_override.hpp"
#include "extractor/maneuver_override_relation_parser.hpp"
#include "extractor/restriction.hpp"
#include "extractor/restriction_parser.hpp"
#include "extractor/scripting_environment_lua.hpp"
#include "extractor/turn_lane_types.hpp"
#include "merger_config.hpp"

namespace osrm
{
namespace merger
{
class Merger
{
  public:
    Merger(MergerConfig merger_config) : config(std::move(merger_config)) {}
    int run(extractor::ScriptingEnvironment &scripting_environment_first, extractor::ScriptingEnvironment &scripting_environment_second);
  private:
    using MapKey = std::tuple<std::string, std::string, std::string, std::string, std::string>;
    using MapVal = unsigned;
    using StringMap = std::unordered_map<MapKey, MapVal>;
    MergerConfig config;

    void parseOSMData(
        StringMap &string_map,
        extractor::ExtractionContainers &extraction_containers,
        extractor::ExtractorCallbacks::ClassesMap &classes_map,
        extractor::LaneDescriptionMap &turn_lane_map,
        extractor::ScriptingEnvironment &scripting_environment,
        const boost::filesystem::path input_path,
        const boost::filesystem::path profile_path,
        const unsigned number_of_threads);

    void writeTimestamp();

    void writeOSMData(
        extractor::ExtractionContainers &extraction_containers,
        extractor::ExtractorCallbacks::ClassesMap &classes_map,
        extractor::ScriptingEnvironment &scripting_environment_first,
        extractor::ScriptingEnvironment &scripting_environment_second);
};
}
}