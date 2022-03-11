#include "osrm/merger.hpp"
#include "osrm/merger_config.hpp"
#include "util/log.hpp"

#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include <cstdlib>

using namespace osrm;

enum class return_code : unsigned
{
    ok,
    fail,
    exit
};

return_code parseArguments(int argc,
                           char *argv[],
                           merger::MergerConfig &merger_config)
{
    // declare a group of options that will be a llowed only on command line
    boost::program_options::options_description generic_options("Options");

    boost::program_options::options_description hidden_options("Hidden options");
    hidden_options.add_options()(
        "input_map_first",
        boost::program_options::value<boost::filesystem::path>(&merger_config.input_path_first),
        "Input file in .osm, .osm.bz2 or .osm.pbf format")(
        "input_profile_first",
        boost::program_options::value<boost::filesystem::path>(&merger_config.profile_path_first),
        "Path to LUA routing profile")(
        "input_map_second",
        boost::program_options::value<boost::filesystem::path>(&merger_config.input_path_second),
        "Input file in .osm, .osm.bz2 or .osm.pbf format")(
        "input_profile_second",
        boost::program_options::value<boost::filesystem::path>(&merger_config.profile_path_second),
        "Path to LUA routing profile");

    // positional option
    boost::program_options::positional_options_description positional_options;
    positional_options.add("input_map_first", 1);
    positional_options.add("input_profile_first", 1);
    positional_options.add("input_map_second", 1);
    positional_options.add("input_profile_second", 1);

    // combine above options for parsing
    boost::program_options::options_description cmdline_options;
    cmdline_options.add(generic_options).add(hidden_options);

    const auto *executable = argv[0];
    boost::program_options::options_description visible_options(
        boost::filesystem::path(executable).filename().string() +
        " <input.osm/.osm.bz2/.osm.pbf> <.lua> <input.osm/.osm.bz2/.osm.pbf> <.lua>");

    // parse command line options
    boost::program_options::variables_map option_variables;
    try
    {
        boost::program_options::store(boost::program_options::command_line_parser(argc, argv)
                                          .options(cmdline_options)
                                          .positional(positional_options)
                                          .run(),
                                      option_variables);
    }
    catch (const boost::program_options::error &e)
    {
        util::Log(logERROR) << e.what();
        return return_code::fail;
    }

    if (!option_variables.count("input_map_first") ||
        !option_variables.count("input_profile_first") ||
        !option_variables.count("input_map_second") ||
        !option_variables.count("input_profile_second"))
    {
        std::cout << visible_options;
        return return_code::exit;
    }

    boost::program_options::notify(option_variables);

    return return_code::ok;
}

int main(int argc, char *argv[])
{
    util::LogPolicy::GetInstance().Unmute();
    merger::MergerConfig merger_config;

    const auto result = parseArguments(argc, argv, merger_config);

    if (return_code::fail == result)
    {
        return EXIT_FAILURE;
    }
    if (return_code::exit == result)
    {
        return EXIT_SUCCESS;
    }

    merger_config.UseDefaultOutputNames(boost::filesystem::path("merged"));

    if (!boost::filesystem::is_regular_file(merger_config.input_path_first))
    {
        util::Log(logERROR) << "Input file " << merger_config.input_path_first.string()
                            << " not found!";
        return EXIT_FAILURE;
    }

    if (!boost::filesystem::is_regular_file(merger_config.profile_path_first))
    {
        util::Log(logERROR) << "Profile " << merger_config.profile_path_first.string()
                            << " not found!";
        return EXIT_FAILURE;
    }

    if (!boost::filesystem::is_regular_file(merger_config.input_path_second))
    {
        util::Log(logERROR) << "Input file " << merger_config.input_path_second.string()
                            << " not found!";
        return EXIT_FAILURE;
    }

    if (!boost::filesystem::is_regular_file(merger_config.profile_path_second))
    {
        util::Log(logERROR) << "Profile " << merger_config.profile_path_second.string()
                            << " not found!";
        return EXIT_FAILURE;
    }

    util::Log(logINFO) << "Starting merge...";

    osrm::merge(merger_config);
}
