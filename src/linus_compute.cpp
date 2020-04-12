#include "application.h"
#include "rowers.h"

#ifndef DEGREES
#define DEGREES 7
#endif

// user_id of LINUS
#ifndef LINUS
#define LINUS 4967
#endif

class LinusDemo : public Application {
public:
  Key commits_key;
  Key users_key;
  Key projects_key;
  set<int> tagged_projects;
  set<int> tagged_users;
  set<int> new_users;

  LinusDemo(const IpV4Addr &ip)
      : Application(ip), commits_key("commits"), users_key("users"),
        projects_key("projects") {
    tagged_users.emplace(LINUS);
    new_users.emplace(LINUS); // should linus be included?
  }

  void find_collabs(int step) {
    cout << "Step: " << step << endl;

    /**
     * SELECT col0 project_id
     * FROM commits
     * WHERE author_id IN new_users AND project_id NOT IN tagged_projects
     */
    auto proj_rower = make_shared<SearchIntIntRower>(0, 1, new_users);

    cluster.map(commits_key, proj_rower);
    const set<int> &new_proj_res = proj_rower->get_results();
    tagged_projects.insert(new_proj_res.begin(), new_proj_res.end());

    cout << "  tagged projects(new: " << new_proj_res.size()
         << ", total: " << tagged_projects.size() << ")" << endl;

    /**
     * SELECT author_id
     * FROM commits
     * WHERE project_id IN new_projects AND author_id NOT IN tagged_users
     */
    auto collabs_rower = make_shared<SearchIntIntRower>(1, 0, new_proj_res);
    cluster.map(commits_key, collabs_rower);
    auto &new_users_res = collabs_rower->get_results();
    new_users.clear();
    new_users.insert(new_users_res.begin(), new_users_res.end());
    tagged_users.insert(new_users_res.begin(), new_users_res.end());

    cout << "  tagged users(new: " << new_users_res.size()
         << ", total: " << tagged_users.size() << ")" << endl;
  }

  void run() {
    for (int i = 0; i < DEGREES; i++)
      find_collabs(i);
  }
};

IpV4Addr get_ip(int argc, char **argv) {
  assert(argc >= 3);

  if (strcmp(argv[1], "--ip") == 0) {
    return IpV4Addr(argv[2]);
  } else if (argc > 4 && strcmp(argv[3], "--ip") == 0) {
    return IpV4Addr(argv[4]);
  }
  assert(false);
}

int main(int argc, char **argv) {
  cout << "starting with: ";
  for (int i = 0; i < argc; i++) {
    cout << argv[i] << ' ';
  }
  cout << endl;

  LinusDemo d(get_ip(argc, argv));
  d.run();
}
