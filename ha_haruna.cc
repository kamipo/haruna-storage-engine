#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#define MYSQL_SERVER 1

#include <mysql/plugin.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <my_dir.h>
#include <my_global.h>
#include <sql_priv.h>
#include <sql_class.h>                          // SSV
#include <probes_mysql.h>

#include <vector>
#include <string>
#include <sstream>
using namespace std;

#include <pficommon/network/http.h>
#include <pficommon/text/json.h>
using namespace pfi::network::http;
using namespace pfi::text::json;

class ha_haruna: public handler
{
public:
  struct Tweet
  {
    ~Tweet()
    {
      delete id;
      delete text;
      delete screen_name;
      delete created_at;
    }

    Tweet(): next(0) {}

    Tweet(Tweet const& that)
      : next(0), id(that.id), text(that.text),
      screen_name(that.screen_name),
      favorites_count(that.favorites_count),
      retweet_count(that.retweet_count),
      created_at(that.created_at) {}

    Tweet(String* id, String* text, String* screen_name,
        long favorites_count, long retweet_count, String* created_at)
      : next(0), id(id), text(text), screen_name(screen_name),
      favorites_count(favorites_count), retweet_count(retweet_count), created_at(created_at) {}

    static void* operator new(size_t size, MEM_ROOT* mem_root) throw ()
    {
      return (void *)alloc_root(mem_root, (uint)size);
    }

    static void operator delete(void* p, size_t size)
    {
      (void)p, (void)size;
      TRASH(p, size);
    }

    Tweet* next;

    String* id;
    String* text;
    String* screen_name;
    long favorites_count;
    long retweet_count;
    String* created_at;
  };

  struct Tweets
  {
    Tweet* first;
    Tweet* last;

    void free()
    {
      for (Tweet *next, *t = first; t; t = next) {
        next = t->next;
        delete t;
      }
      first = last = 0;
    }

    void append(Tweet* t)
    {
      if (!last) {
        first = t;
      } else {
        last->next = t;
      }
      last = t;
    }

    Tweets& operator=(Tweets const& that)
    {
      first = that.first;
      last = that.last;
      return *this;
    }

    Tweets(): first(0), last(0) {}
  };

public:
  ha_haruna(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg), cur(0), current_timeline(), with_mariko(false)
  {
    thr_lock_init(&_lock);
  }

  ~ha_haruna();

  const char *table_type() const { return "Haruna"; }
  const char *index_type(uint inx) { return "NONE"; }

  const char **bas_ext() const
  {
    return exts;
  }

  ulonglong table_flags() const
  {
    return (HA_NO_TRANSACTIONS | HA_REC_NOT_IN_SEQ | HA_NO_AUTO_INCREMENT |
        HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |
        HA_CAN_REPAIR);
  }

  ulong index_flags(uint idx, uint part, bool all_parts) const
  {
    return 0;
  }

  uint max_record_length() const { return HA_MAX_REC_LENGTH; }
  uint max_keys()          const { return 0; }
  uint max_key_parts()     const { return 0; }
  uint max_key_length()    const { return 0; }
  double scan_time() { return (double) (stats.records+stats.deleted) / 20.0+10; }
  bool fast_key_read() { return 1;}
  ha_rows estimate_rows_upper_bound() { return HA_POS_ERROR; }

  int open(const char *name, int mode, uint open_options)
  {
    if ( strstr(name, "mariko") == NULL ) {
      with_mariko = false;
    } else {
      with_mariko = true;
    }
    thr_lock_data_init(&_lock, &lock, NULL);
    return 0;
  }

  int close(void)
  {
    return 0;
  }

  int write_row(uchar * buf)
  {
    return 0;
  }

  int update_row(const uchar * old_data, uchar * new_data)
  {
    return 0;
  }

  int delete_row(const uchar * buf)
  {
    return 0;
  }

  int rnd_init(bool scan = false)
  {
    current_timeline = get_timeline();
    cur = current_timeline.first;
    return 0;
  }

  int rnd_next(uchar *buf)
  {
    const bool read_all = !bitmap_is_clear_all(table->write_set);

    MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

    if (!cur) {
      MYSQL_READ_ROW_DONE(HA_ERR_END_OF_FILE);
      return HA_ERR_END_OF_FILE;
    }
    for (Field **field=table->field ; *field ; field++) {
      if (read_all || bitmap_is_set(table->read_set, (*field)->field_index)) {
        if (strcmp((*field)->field_name, "id") == 0) {
          (*field)->set_notnull();
          (*field)->store(cur->id->ptr(), cur->id->length(), cur->id->charset(), CHECK_FIELD_WARN);
        } else if (strcmp((*field)->field_name, "text") == 0) {
          (*field)->set_notnull();
          (*field)->store(cur->text->ptr(), cur->text->length(), cur->text->charset(), CHECK_FIELD_WARN);
        } else if (strcmp((*field)->field_name, "screen_name") == 0) {
          (*field)->set_notnull();
          (*field)->store(cur->screen_name->ptr(), cur->screen_name->length(), cur->screen_name->charset(), CHECK_FIELD_WARN);
        } else if (strcmp((*field)->field_name, "favorites_count") == 0) {
          (*field)->set_notnull();
          (*field)->store(cur->favorites_count, CHECK_FIELD_WARN);
        } else if (strcmp((*field)->field_name, "retweet_count") == 0) {
          (*field)->set_notnull();
          (*field)->store(cur->retweet_count, CHECK_FIELD_WARN);
        } else if (strcmp((*field)->field_name, "created_at") == 0) {
          (*field)->set_notnull();
          (*field)->store(cur->created_at->ptr(), cur->created_at->length(), cur->created_at->charset(), CHECK_FIELD_WARN);
        } else {
          (*field)->set_null();
        }
      }
    }
    if (!cur->next) {
      Tweets timeline = get_timeline(cur->id);
      current_timeline.free();
      current_timeline = timeline;
      if (current_timeline.first) {
        cur = current_timeline.first->next;
      } else {
        cur = 0;
      }
    } else {
      cur = cur->next;
    }
    MYSQL_READ_ROW_DONE(0);
    return 0;
  }

  int rnd_pos(uchar * buf, uchar *pos)
  {
    return 0;
  }

  int rnd_end()
  {
    cur = 0;
    current_timeline.free();
    return 0;
  }

  void position(const uchar *record)
  {
  }

  int info(uint)
  {
    return 0;
  }

  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info)
  {
    return 0;
  }

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
      enum thr_lock_type lock_type)
  {
    if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
      lock.type = lock_type;
    *to++ = &lock;
    return to;
  }

private:
  Tweets get_timeline(String const *max_id = 0)
  {
    Tweets retval;

    String url("http://api.twitter.com", &my_charset_utf8_bin);

    if (with_mariko) {
      url.append("/1/lists/statuses.json?slug=kojimari&owner_screen_name=kamipo");
    } else {
      url.append("/1/statuses/user_timeline.json?screen_name=kojiharunyan");
    }

    if (max_id) {
      url.append("&max_id=");
      url.append(*max_id);
    }

    try {
      httpstream hs(url.ptr());

      stringstream ss;
      json js;

      for (string line; getline(hs, line); )
        ss << line;

      ss >> js;

      if (!is<json_array>(js))
        return retval;

      vector<json> tl = json_cast<vector<json> >(js);
      vector<json>::iterator it = tl.begin();

      for (; it != tl.end(); ++it) {
        String* id;
        String* text;
        String* screen_name;
        long favorites_count = 0;
        long retweet_count = 0;
        String* created_at;

        string _id = json_cast<string>((*it)["id_str"]);
        id = new(current_thd->mem_root) String(_id.c_str(), _id.size(), &my_charset_utf8_bin);
        id->copy();

        string _text = json_cast<string>((*it)["text"]);
        text = new(current_thd->mem_root) String(_text.c_str(), _text.size(), &my_charset_utf8_bin);
        text->copy();

        string _screen_name = json_cast<string>((*it)["user"]["screen_name"]);
        screen_name = new(current_thd->mem_root) String(_screen_name.c_str(), _screen_name.size(), &my_charset_utf8_bin);
        screen_name->copy();

        json fav = (*it)["favorites_count"];
        favorites_count = is<json_integer>(fav) ? json_cast<int>(fav) : 0;

        json rt = (*it)["retweet_count"];
        retweet_count = is<json_integer>(rt) ? json_cast<int>(rt) : 0;

        struct tm t;
        string _created_at = json_cast<string>((*it)["created_at"]);
        strptime(_created_at.c_str(), "%a %b %d %H:%M:%S %z %Y", &t);
        created_at = new(current_thd->mem_root) String();
        created_at->alloc(20);
        created_at->length(strftime((char *)created_at->ptr(), created_at->alloced_length(), "%Y-%m-%d %H:%M:%S", &t));

        retval.append(new(current_thd->mem_root) Tweet(id, text, screen_name, favorites_count, retweet_count, created_at));
      }
    } catch (...) {
    }

    return retval;
  }

private:
  THR_LOCK _lock;
  THR_LOCK_DATA lock;
  Tweet* cur;
  Tweets current_timeline;
  bool with_mariko;

private:
  static const char *exts[];
};

ha_haruna::~ha_haruna()
{
    current_timeline.free();
    thr_lock_delete(&_lock);
}

static handler* haruna_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
  return new(mem_root) ha_haruna(hton, table);
}

static int haruna_init(void *p)
{
  handlerton *hton = (handlerton *)p;

  hton->state = SHOW_OPTION_YES;
  hton->db_type = DB_TYPE_UNKNOWN;
  hton->create = haruna_create_handler;
  hton->flags = HTON_CAN_RECREATE;

  return 0;
}

static int haruna_done(void *p)
{
  return 0;
}

/*
  If frm_error() is called in table.cc this is called to find out what file
  extensions exist for this handler.
*/
const char *ha_haruna::exts[] = {
  NullS
};

struct st_mysql_storage_engine haruna_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(haruna)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &haruna_storage_engine,
  "Haruna",
  "Ryuta Kamizono",
  "Haruna storage engine",
  PLUGIN_LICENSE_GPL,
  haruna_init, /* Plugin Init */
  haruna_done, /* Plugin Deinit */
  0x0100 /* 1.0 */,
  NULL,                       /* status variables                */
  NULL,                       /* system variables                */
  NULL,                       /* config options                  */
  0,                          /* flags                           */
}
mysql_declare_plugin_end;

