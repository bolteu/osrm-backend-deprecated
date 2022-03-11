#include "merger/merger.hpp"
#include "extractor/class_data.hpp"
#include "extractor/extractor_callbacks.hpp"
#include "extractor/extraction_node.hpp"
#include "extractor/extraction_relation.hpp"
#include "extractor/extraction_way.hpp"
#include "extractor/files.hpp"

#include "util/log.hpp"
#include "util/timing_util.hpp"
#include "util/typedefs.hpp"

#include <osmium/handler/node_locations_for_ways.hpp>
#include <osmium/index/map/flex_mem.hpp>
#include <osmium/io/any_input.hpp>
#include <osmium/visitor.hpp>

#if TBB_VERSION_MAJOR == 2020
#include <tbb/global_control.h>
#else
#include <tbb/task_scheduler_init.h>
#endif
#include <tbb/pipeline.h>

namespace osrm
{
namespace merger
{

namespace
{
// Converts the class name map into a fixed mapping of index to name
void SetClassNames(const std::vector<std::string> &class_names,
                   extractor::ExtractorCallbacks::ClassesMap &classes_map,
                   extractor::ProfileProperties &profile_properties)
{
    // if we get a list of class names we can validate if we set invalid classes
    // and add classes that were never reference
    if (!class_names.empty())
    {
        // add class names that were never used explicitly on a way
        // this makes sure we can correctly validate unkown class names later
        for (const auto &name : class_names)
        {
            if (!extractor::isValidClassName(name))
            {
                throw util::exception("Invalid class name " + name + " only [a-Z0-9] allowed.");
            }

            auto iter = classes_map.find(name);
            if (iter == classes_map.end())
            {
                auto index = classes_map.size();
                if (index > extractor::MAX_CLASS_INDEX)
                {
                    throw util::exception("Maximum number of classes is " +
                                          std::to_string(extractor::MAX_CLASS_INDEX + 1));
                }

                classes_map[name] = extractor::getClassData(index);
            }
        }

        // check if class names are only from the list supplied by the user
        for (const auto &pair : classes_map)
        {
            auto iter = std::find(class_names.begin(), class_names.end(), pair.first);
            if (iter == class_names.end())
            {
                throw util::exception("Profile used unknown class name: " + pair.first);
            }
        }
    }

    for (const auto &pair : classes_map)
    {
        auto range = extractor::getClassIndexes(pair.second);
        BOOST_ASSERT(range.size() == 1);
        profile_properties.SetClassName(range.front(), pair.first);
    }
}

// Converts the class name list to a mask list
void SetExcludableClasses(const extractor::ExtractorCallbacks::ClassesMap &classes_map,
                          const std::vector<std::vector<std::string>> &excludable_classes,
                          extractor::ProfileProperties &profile_properties)
{
    if (excludable_classes.size() > extractor::MAX_EXCLUDABLE_CLASSES)
    {
        throw util::exception("Only " + std::to_string(extractor::MAX_EXCLUDABLE_CLASSES) +
                              " excludable combinations allowed.");
    }

    // The exclude index 0 is reserve for not excludeing anything
    profile_properties.SetExcludableClasses(0, 0);

    std::size_t combination_index = 1;
    for (const auto &combination : excludable_classes)
    {
        extractor::ClassData mask = 0;
        for (const auto &name : combination)
        {
            auto iter = classes_map.find(name);
            if (iter == classes_map.end())
            {
                util::Log(logWARNING)
                    << "Unknown class name " + name + " in excludable combination. Ignoring.";
            }
            else
            {
                mask |= iter->second;
            }
        }

        if (mask > 0)
        {
            profile_properties.SetExcludableClasses(combination_index++, mask);
        }
    }
}
} // namespace

int Merger::run(extractor::ScriptingEnvironment &scripting_environment_first, extractor::ScriptingEnvironment &scripting_environment_second)
{
    TIMER_START(extracting);

    StringMap string_map;
    extractor::ExtractionContainers extraction_containers;
    extractor::ExtractorCallbacks::ClassesMap classes_map;
    extractor::LaneDescriptionMap turn_lane_map;

    parseOSMData(
        string_map,
        extraction_containers,
        classes_map,
        turn_lane_map,
        scripting_environment_first,
        config.input_path_first,
        config.profile_path_first,
        2);
    parseOSMData(
        string_map,
        extraction_containers,
        classes_map,
        turn_lane_map,
        scripting_environment_second,
        config.input_path_second,
        config.profile_path_second,
        2);

    util::Log() << "==== Classes map result ====";
    for (std::pair<std::string, extractor::ClassData> element : classes_map)
    {
        util::Log() << element.first << " :: " << unsigned(element.second);
    }

    writeTimestamp();
    writeOSMData(
        extraction_containers,
        classes_map,
        scripting_environment_first,
        scripting_environment_second);

    TIMER_STOP(extracting);
    util::Log() << "extraction finished after " << TIMER_SEC(extracting) << "s";

    util::Log(logINFO) << "Merge is done!";
    return 0;
}

void Merger::parseOSMData(
    StringMap &string_map,
    extractor::ExtractionContainers &extraction_containers,
    extractor::ExtractorCallbacks::ClassesMap &classes_map,
    extractor::LaneDescriptionMap &turn_lane_map,
    extractor::ScriptingEnvironment &scripting_environment,
    const boost::filesystem::path input_path,
    const boost::filesystem::path profile_path,
    const unsigned number_of_threads)
{
    util::Log() << "Input file: " << input_path.filename().string();
    if (!profile_path.empty())
    {
        util::Log() << "Profile: " << profile_path.filename().string();
    }
    util::Log() << "Threads: " << number_of_threads;

    const osmium::io::File input_file(input_path.string());
    osmium::thread::Pool pool(number_of_threads);

    util::Log() << "Parsing in progress..";
    TIMER_START(parsing);

    { // Parse OSM header
        osmium::io::Reader reader(input_file, pool, osmium::osm_entity_bits::nothing);
        osmium::io::Header header = reader.header();

        std::string generator = header.get("generator");
        if (generator.empty())
        {
            generator = "unknown tool";
        }
        util::Log() << "input file generated by " << generator;
    }

    // Extraction containers and restriction parser
    auto extractor_callbacks =
        std::make_unique<extractor::ExtractorCallbacks>(string_map,
                                            extraction_containers,
                                            classes_map,
                                            turn_lane_map,
                                            scripting_environment.GetProfileProperties());

    // get list of supported relation types
    auto relation_types = scripting_environment.GetRelations();
    std::sort(relation_types.begin(), relation_types.end());

    std::vector<std::string> restrictions = scripting_environment.GetRestrictions();
    // setup restriction parser
    const extractor::RestrictionParser restriction_parser(
        scripting_environment.GetProfileProperties().use_turn_restrictions,
        config.parse_conditionals,
        restrictions);

    const extractor::ManeuverOverrideRelationParser maneuver_override_parser;

    // OSM data reader
    using SharedBuffer = std::shared_ptr<osmium::memory::Buffer>;
    struct ParsedBuffer
    {
        SharedBuffer buffer;
        std::vector<std::pair<const osmium::Node &, extractor::ExtractionNode>> resulting_nodes;
        std::vector<std::pair<const osmium::Way &, extractor::ExtractionWay>> resulting_ways;
        std::vector<std::pair<const osmium::Relation &, extractor::ExtractionRelation>> resulting_relations;
        std::vector<extractor::InputTurnRestriction> resulting_restrictions;
        std::vector<extractor::InputManeuverOverride> resulting_maneuver_overrides;
    };

    extractor::ExtractionRelationContainer relations;

    const auto buffer_reader = [](osmium::io::Reader &reader) {
        return tbb::filter_t<void, SharedBuffer>(
            tbb::filter::serial_in_order, [&reader](tbb::flow_control &fc) {
                if (auto buffer = reader.read())
                {
                    return std::make_shared<osmium::memory::Buffer>(std::move(buffer));
                }
                else
                {
                    fc.stop();
                    return SharedBuffer{};
                }
            });
    };

    // Node locations cache (assumes nodes are placed before ways)
    using osmium_index_type =
        osmium::index::map::FlexMem<osmium::unsigned_object_id_type, osmium::Location>;
    using osmium_location_handler_type = osmium::handler::NodeLocationsForWays<osmium_index_type>;

    osmium_index_type location_cache;
    osmium_location_handler_type location_handler(location_cache);

    tbb::filter_t<SharedBuffer, SharedBuffer> location_cacher(
        tbb::filter::serial_in_order, [&location_handler](SharedBuffer buffer) {
            osmium::apply(buffer->begin(), buffer->end(), location_handler);
            return buffer;
        });

    // OSM elements Lua parser
    tbb::filter_t<SharedBuffer, ParsedBuffer> buffer_transformer(
        tbb::filter::parallel, [&](const SharedBuffer buffer) {
            ParsedBuffer parsed_buffer;
            parsed_buffer.buffer = buffer;
            scripting_environment.ProcessElements(*buffer,
                                                  restriction_parser,
                                                  maneuver_override_parser,
                                                  relations,
                                                  parsed_buffer.resulting_nodes,
                                                  parsed_buffer.resulting_ways,
                                                  parsed_buffer.resulting_restrictions,
                                                  parsed_buffer.resulting_maneuver_overrides);
            return parsed_buffer;
        });

    // Parsed nodes and ways handler
    unsigned number_of_nodes = 0;
    unsigned number_of_ways = 0;
    unsigned number_of_restrictions = 0;
    unsigned number_of_maneuver_overrides = 0;
    tbb::filter_t<ParsedBuffer, void> buffer_storage(
        tbb::filter::serial_in_order, [&](const ParsedBuffer &parsed_buffer) {
            number_of_nodes += parsed_buffer.resulting_nodes.size();
            // put parsed objects thru extractor callbacks
            for (const auto &result : parsed_buffer.resulting_nodes)
            {
                extractor_callbacks->ProcessNode(result.first, result.second);
            }
            number_of_ways += parsed_buffer.resulting_ways.size();
            for (const auto &result : parsed_buffer.resulting_ways)
            {
                extractor_callbacks->ProcessWay(result.first, result.second);
            }

            number_of_restrictions += parsed_buffer.resulting_restrictions.size();
            for (const auto &result : parsed_buffer.resulting_restrictions)
            {
                extractor_callbacks->ProcessRestriction(result);
            }

            number_of_maneuver_overrides = parsed_buffer.resulting_maneuver_overrides.size();
            for (const auto &result : parsed_buffer.resulting_maneuver_overrides)
            {
                extractor_callbacks->ProcessManeuverOverride(result);
            }
        });

    tbb::filter_t<SharedBuffer, std::shared_ptr<extractor::ExtractionRelationContainer>> buffer_relation_cache(
        tbb::filter::parallel, [&](const SharedBuffer buffer) {
            if (!buffer)
                return std::shared_ptr<extractor::ExtractionRelationContainer>{};

            auto relations = std::make_shared<extractor::ExtractionRelationContainer>();
            for (auto entity = buffer->cbegin(), end = buffer->cend(); entity != end; ++entity)
            {
                if (entity->type() != osmium::item_type::relation)
                    continue;

                const auto &rel = static_cast<const osmium::Relation &>(*entity);

                const char *rel_type = rel.get_value_by_key("type");
                if (!rel_type || !std::binary_search(relation_types.begin(),
                                                     relation_types.end(),
                                                     std::string(rel_type)))
                    continue;

                extractor::ExtractionRelation extracted_rel({rel.id(), osmium::item_type::relation});
                for (auto const &t : rel.tags())
                    extracted_rel.attributes.emplace_back(std::make_pair(t.key(), t.value()));

                for (auto const &m : rel.members())
                {
                    extractor::ExtractionRelation::OsmIDTyped const mid(m.ref(), m.type());
                    extracted_rel.AddMember(mid, m.role());
                    relations->AddRelationMember(extracted_rel.id, mid);
                }

                relations->AddRelation(std::move(extracted_rel));
            };
            return relations;
        });

    unsigned number_of_relations = 0;
    tbb::filter_t<std::shared_ptr<extractor::ExtractionRelationContainer>, void> buffer_storage_relation(
        tbb::filter::serial_in_order,
        [&](const std::shared_ptr<extractor::ExtractionRelationContainer> parsed_relations) {
            number_of_relations += parsed_relations->GetRelationsNum();
            relations.Merge(std::move(*parsed_relations));
        });

    // Parse OSM elements with parallel transformer
    // Number of pipeline tokens that yielded the best speedup was about 1.5 * num_cores
    const auto num_threads = std::thread::hardware_concurrency() * 1.5;
    const auto read_meta =
        config.use_metadata ? osmium::io::read_meta::yes : osmium::io::read_meta::no;

    { // Relations reading pipeline
        util::Log() << "Parse relations ...";
        osmium::io::Reader reader(input_file, pool, osmium::osm_entity_bits::relation, read_meta);
        tbb::parallel_pipeline(
            num_threads, buffer_reader(reader) & buffer_relation_cache & buffer_storage_relation);
    }

    { // Nodes and ways reading pipeline
        util::Log() << "Parse ways and nodes ...";
        osmium::io::Reader reader(input_file,
                                  pool,
                                  osmium::osm_entity_bits::node | osmium::osm_entity_bits::way |
                                      osmium::osm_entity_bits::relation,
                                  read_meta);

        const auto pipeline =
            scripting_environment.HasLocationDependentData() && config.use_locations_cache
                ? buffer_reader(reader) & location_cacher & buffer_transformer & buffer_storage
                : buffer_reader(reader) & buffer_transformer & buffer_storage;
        tbb::parallel_pipeline(num_threads, pipeline);
    }

    TIMER_STOP(parsing);
    util::Log() << "Parsing finished after " << TIMER_SEC(parsing) << " seconds";

    util::Log() << "Raw input contains " << number_of_nodes << " nodes, " << number_of_ways
                << " ways, and " << number_of_relations << " relations, " << number_of_restrictions
                << " restrictions";

    util::Log() << "String map size " << string_map.size();
    util::Log() << "Classes map size " << classes_map.size();
    util::Log() << "Turn lane map size " << turn_lane_map.data.size();

    extractor_callbacks.reset();

    if (extraction_containers.all_edges_list.empty())
    {
        throw util::exception(std::string("There are no edges remaining after parsing.") +
                              SOURCE_REF);
    }
}

void Merger::writeTimestamp()
{
    // write .timestamp data file
    std::string timestamp = config.data_version;

    extractor::files::writeTimestamp(config.GetPath(".osrm.timestamp").string(), timestamp);
    if (timestamp.empty())
    {
        timestamp = "n/a";
    }
    util::Log() << "timestamp: " << timestamp;
}

void Merger::writeOSMData(
    extractor::ExtractionContainers &extraction_containers,
    extractor::ExtractorCallbacks::ClassesMap &classes_map,
    extractor::ScriptingEnvironment &scripting_environment_first,
    extractor::ScriptingEnvironment &scripting_environment_second)
{
    // just to get it to compile
    scripting_environment_second.GetProfileProperties();
    // TODO: use first scripting_environment for now
    extraction_containers.PrepareData(scripting_environment_first,
                                      config.GetPath(".osrm").string(),
                                      config.GetPath(".osrm.names").string());

    auto profile_properties = scripting_environment_first.GetProfileProperties();
    SetClassNames(scripting_environment_first.GetClassNames(), classes_map, profile_properties);
    auto excludable_classes = scripting_environment_first.GetExcludableClasses();
    SetExcludableClasses(classes_map, excludable_classes, profile_properties);
    extractor::files::writeProfileProperties(config.GetPath(".osrm.properties").string(), profile_properties);
}

} // namespace merger
} // namespace osrm