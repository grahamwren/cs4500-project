#include "application.h"
#include "rowers.h"

#ifndef DEGREES
#define DEGREES 1
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
  set<int> collabs;

  LinusDemo(const IpV4Addr &ip)
      : Application(ip), commits_key("commits"), users_key("users"),
        projects_key("projects") {
    collabs.emplace(LINUS);
  }

  void ensure_keys_removed() {
    cluster.remove(commits_key);
    cluster.remove(users_key);
    cluster.remove(projects_key);
  }

  void fill_cluster() {
    bool import_res;
    import_res = cluster.load_file(projects_key, "/datasets/projects.ltgt");
    cout << "Loaded: projects" << endl;
    assert(import_res);
    import_res = cluster.load_file(commits_key, "/datasets/commits.ltgt");
    cout << "Loaded: commits" << endl;
    assert(import_res);
    import_res = cluster.load_file(users_key, "/datasets/users.ltgt");
    cout << "Loaded: users" << endl;
    assert(import_res);
  }

  void find_collabs(int step) {
    cout << "Step: " << step << endl;

    set<int> cols;
    cols.emplace(1); // author_id
    cols.emplace(2); // committer_id

    /**
     * SELECT col0 project_id
     * FROM commits
     * WHERE author_id IN collabs
     *    OR committer_id IN collabs
     */
    auto proj_rower =
        make_shared<SearchIntIntRower>(set<int>{0}, set<int>{1, 2}, collabs);

    cluster.map(commits_key, proj_rower);
    const set<int> &proj_res = proj_rower->get_results();
    cout << "  tagged projects: [";
    auto pit = proj_res.begin();
    if (pit != proj_res.end())
      cout << *pit++;
    for (; pit != proj_res.end(); pit++)
      cout << ", " << *pit;
    cout << "]" << endl;

    /**
     * SELECT author_id FROM commits WHERE project_id IN proj_res
     * UNION DISTINCT
     * SELECT committer_id FROM commits WHERE project_id IN proj_res
     */
    auto collabs_rower =
        make_shared<SearchIntIntRower>(set<int>{1, 2}, set<int>{0}, proj_res);
    cluster.map(commits_key, collabs_rower);
    collabs.swap(collabs_rower->get_results());

    cout << "  tagged users: [";
    auto cit = collabs.begin();
    if (cit != collabs.end())
      cout << *cit++;
    for (; cit != collabs.end(); cit++)
      cout << ", " << *cit;
    cout << "]" << endl;
  }

  void run() {
    ensure_keys_removed();
    fill_cluster();
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
