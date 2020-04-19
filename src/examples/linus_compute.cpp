#include "lib/rowers.h"
#include "sdk/application.h"
#include "utils/cli_flags.h"

#ifndef DEGREES
#define DEGREES 7
#endif

// user_id of LINUS
#ifndef LINUS
#define LINUS 4967
#endif

/**
 * EAU2 cluster application to perform the "7 degrees of Linus" computation.
 * Expects a DataFrame to exist under Key("commits") with the following schema:
 *
 * Schema([III], columns: ['ProjectId', 'AuthorId', 'CommitterId']
 *
 * authors: @grahamwren, @jagen31
 */
class LinusDemo : public Application {
public:
  Key commits_key;
  Key users_key;
  Key projects_key;
  set<int> tagged_projects;
  set<int> tagged_users;

  LinusDemo(const IpV4Addr &ip)
      : Application(ip), commits_key("commits"), users_key("users"),
        projects_key("projects") {
    /* assert the "commits" DF already loaded into the Cluster */
    assert(cluster.get_df_info(commits_key));

    /* add Linus as first tagged users to search for */
    tagged_users.emplace(LINUS);
  }

  void find_collabs(int step) {
    cout << "Step: " << step << endl;

    /**
     * Find all projects contributed to by the currently tagged users:
     *
     * SELECT col0 project_id
     * FROM commits
     * WHERE author_id IN tagged_users
     */
    auto proj_rower = make_shared<SearchIntIntRower>(0, 1, tagged_users);

    cluster.map(commits_key, proj_rower);
    const set<int> &new_proj_res = proj_rower->get_results();
    tagged_projects.insert(new_proj_res.begin(), new_proj_res.end());

    cout << "  tagged projects(";
    for (auto e : tagged_projects)
      cout << e << ",";
    cout << ")" << endl;

    /**
     * Find all users who have contributed to the projects we found above:
     *
     * SELECT author_id
     * FROM commits
     * WHERE project_id IN new_projects
     */
    auto collabs_rower = make_shared<SearchIntIntRower>(1, 0, tagged_projects);
    cluster.map(commits_key, collabs_rower);
    auto &new_users_res = collabs_rower->get_results();
    tagged_users.insert(new_users_res.begin(), new_users_res.end());

    cout << "  tagged users(";
    if (tagged_users.size() > 40)
      cout << tagged_users.size() << " users";
    else {
      auto it = tagged_users.begin();
      if (tagged_users.size() > 0)
        cout << *it++;
      while (it != tagged_users.end())
        cout << "," << *it++;
    }
    cout << ")" << endl;
  }

  void run() {
    for (int i = 0; i < DEGREES; i++)
      find_collabs(i);
  }
};

int main(int argc, char **argv) {
  CliFlags cli;
  cli.add_flag("--ip").parse(argc, argv, true);
  auto ip = cli.get_flag("--ip");
  assert(ip); // "ip" flag required

  LinusDemo(ip->c_str()).run();
}
