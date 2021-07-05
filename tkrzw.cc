/*************************************************************************************************
 * Ruby binding of Tkrzw.
 *************************************************************************************************/

#include <functional>
#include <string>
#include <string_view>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <cstddef>
#include <cstdint>

#include "tkrzw_cmd_util.h"
#include "tkrzw_dbm.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_dbm_poly.h"
#include "tkrzw_dbm_shard.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_util.h"
#include "tkrzw_key_comparators.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

extern "C" {

#include "ruby.h"
#include "ruby/thread.h"

typedef VALUE (*METHOD)(...);

// Global variables.
VALUE mod_tkrzw;
ID id_obj_to_str;
ID id_obj_to_s;
ID id_obj_to_i;
ID id_str_force_encoding;

VALUE cls_util;
VALUE cls_status;
VALUE cls_expt;
ID id_expt_status;
VALUE cls_dbm;
VALUE cls_iter;
VALUE cls_textfile;

// Generates a string expression of an arbitrary object.
static VALUE StringValueEx(VALUE vobj) {
  switch (TYPE(vobj)) {
    case T_FIXNUM: {
      char kbuf[32];
      size_t ksiz = std::sprintf(kbuf, "%d", (int)FIX2INT(vobj));
      return rb_str_new(kbuf, ksiz);
    }
    case T_BIGNUM: {
      char kbuf[32];
      size_t ksiz = std::sprintf(kbuf, "%lld", (long long)NUM2LL(vobj));
      return rb_str_new(kbuf, ksiz);
    }
    case T_FLOAT: {
      const std::string& str = tkrzw::ToString(NUM2DBL(vobj));
      return rb_str_new(str.data(), str.size());
    }
    case T_NIL: {
      return rb_str_new("", 0);
    }
    case T_TRUE: {
      return rb_str_new("true", 4);
    }
    case T_FALSE: {
      return rb_str_new("false", 5);
    }
  }
  if (rb_respond_to(vobj, id_obj_to_str)) {
    return StringValue(vobj);
  }
  if (rb_respond_to(vobj, id_obj_to_s)) {
    return rb_funcall(vobj, id_obj_to_s, 0);
  }
  char kbuf[64];
  std::sprintf(kbuf, "#<Object:0x%llx>", (long long)rb_obj_id(vobj));
  return rb_str_new2(kbuf);
}

// Gets a string view of a Ruby string. */
static std::string_view GetStringView(VALUE vstr) {
  return std::string_view(RSTRING_PTR(vstr), RSTRING_LEN(vstr));
}

// Gets a integer value of a Ruby object. */
static int64_t GetInteger(VALUE vobj) {
  switch (TYPE(vobj)) {
    case T_FIXNUM: {
      return FIX2LONG(vobj);
    }
    case T_BIGNUM: {
      return NUM2LL(vobj);
    }
    case T_FLOAT: {
      return NUM2DBL(vobj);
    }
    case T_NIL: {
      return 0;
    }
    case T_TRUE: {
      return 1;
    }
    case T_FALSE: {
      return 0;
    }
  }
  if (rb_respond_to(vobj, id_obj_to_i)) {
    return rb_funcall(vobj, id_obj_to_i, 0);
  }
  char kbuf[64];
  std::sprintf(kbuf, "#<Object:0x%llx>", (long long)rb_obj_id(vobj));
  return rb_str_new2(kbuf);
}

// Maps a Ruby hash object into a C++ map.
static std::map<std::string, std::string> HashToMap(VALUE vhash) {
  std::map<std::string, std::string> map;
  if (TYPE(vhash) == T_HASH) {
    ID id_hash_keys = rb_intern("keys");
    volatile VALUE vkeys = rb_funcall(vhash, id_hash_keys, 0);
    const int32_t num_keys = RARRAY_LEN(vkeys);
    for (int32_t i = 0; i < num_keys; i++) {
      volatile VALUE vkey = rb_ary_entry(vkeys, i);
      volatile VALUE vvalue = rb_hash_aref(vhash, vkey);
      vkey = StringValueEx(vkey);
      vvalue = StringValueEx(vvalue);
      map.emplace(std::string(RSTRING_PTR(vkey), RSTRING_LEN(vkey)),
                  std::string(RSTRING_PTR(vvalue), RSTRING_LEN(vvalue)));
    }
  }
  return map;
}

// Extracts a list of pairs of string views from an array object.
static std::vector<std::pair<std::string_view, std::string_view>> ExtractSVPairs(VALUE varray) {
  std::vector<std::pair<std::string_view, std::string_view>> result;
  const size_t size = RARRAY_LEN(varray);
  result.reserve(size);
  for (size_t i = 0; i < size; i++) {
    volatile VALUE vpair = rb_ary_entry(varray, i);
    if (TYPE(vpair) == T_ARRAY && RARRAY_LEN(vpair) >= 2) {
      volatile VALUE vkey = rb_ary_entry(vpair, 0);
      volatile VALUE vvalue = rb_ary_entry(vpair, 1);
      vkey = StringValueEx(vkey);
      std::string_view key_view(RSTRING_PTR(vkey), RSTRING_LEN(vkey));
      std::string_view value_view;
      if (vvalue != Qnil) {
        vvalue = StringValueEx(vvalue);
        value_view = std::string_view(RSTRING_PTR(vvalue), RSTRING_LEN(vvalue));
      }
      result.emplace_back(std::make_pair(key_view, value_view));
    }
  }
  return result;
}

// Define the module.
static void DefineModule() {
  mod_tkrzw = rb_define_module("Tkrzw");
  id_obj_to_str = rb_intern("to_str");
  id_obj_to_s = rb_intern("to_s");
  id_obj_to_i = rb_intern("to_i");  
  id_str_force_encoding = rb_intern("force_encoding");
}

// Makes a string object in the internal encoding of the database.
static VALUE MakeString(std::string_view str, VALUE venc) {
  volatile VALUE vstr = rb_str_new(str.data(), str.size());
  if (venc != Qnil) {
    rb_funcall(vstr, id_str_force_encoding, 1, venc);
  }
  return vstr;
}

// Wrapper of a native function.
class NativeFunction {
 public:
  NativeFunction(bool concurrent, std::function<void(void)> func)
      : func_(std::move(func)) {
    if (concurrent) {
      rb_thread_call_without_gvl(Run, this, RUBY_UBF_IO, nullptr);
    } else {
      func_();
    }
  }

  static void* Run(void* param) {
    ((NativeFunction*)param)->func_();
    return nullptr;
  }

 private:
  std::function<void(void)> func_;
};

// Yields the process to the given block.
static VALUE YieldToBlock(VALUE args) {
  return rb_yield(args);
}

// Gets an Encoding object of a name.
static VALUE GetEncoding(std::string_view name) {
  ID id_enc_find = rb_intern("find");
  return rb_funcall(rb_cEncoding, id_enc_find, 1, rb_str_new(name.data(), name.size()));
}

// Ruby wrapper of the DBM object.
struct StructDBM {
  std::unique_ptr<tkrzw::ParamDBM> dbm;
  bool concurrent = false;
  volatile VALUE venc = Qnil;
};

// Ruby wrapper of the Iterator object.
struct StructIter {
  std::unique_ptr<tkrzw::DBM::Iterator> iter;
  bool concurrent = false;
  volatile VALUE venc = Qnil;
};

// Ruby wrapper of the TextFile object.
struct StructTextFile {
  std::unique_ptr<tkrzw::File> file;
  bool concurrent = false;
  volatile VALUE venc = Qnil;
  explicit StructTextFile(tkrzw::File* file) : file(file) {}
};

// Implementation of Utility.get_memory_usage.
static VALUE util_get_memory_usage(VALUE vself) {
  const std::map<std::string, std::string> records = tkrzw::GetSystemInfo();
  return LL2NUM(tkrzw::StrToInt(tkrzw::SearchMap(records, "mem_rss", "-1")));
}

// Implementation of Utility.primary_hash.
static VALUE util_primary_hash(int argc, VALUE* argv, VALUE vself) {
  volatile VALUE vdata, vnum;
  rb_scan_args(argc, argv, "11", &vdata, &vnum);
  vdata = StringValueEx(vdata);
  const std::string_view data = GetStringView(vdata);
  uint64_t num_buckets = vnum == Qnil ? 0 : NUM2ULL(vnum);
  if (num_buckets == 0) {
    num_buckets = tkrzw::UINT64MAX;
  }
  return ULL2NUM(tkrzw::PrimaryHash(data, num_buckets));
}

// Implementation of Utility.secondary_hash.
static VALUE util_secondary_hash(int argc, VALUE* argv, VALUE vself) {
  volatile VALUE vdata, vnum;
  rb_scan_args(argc, argv, "11", &vdata, &vnum);
  vdata = StringValueEx(vdata);
  const std::string_view data = GetStringView(vdata);
  uint64_t num_buckets = vnum == Qnil ? 0 : NUM2ULL(vnum);
  if (num_buckets == 0) {
    num_buckets = tkrzw::UINT64MAX;
  }
  return ULL2NUM(tkrzw::SecondaryHash(data, num_buckets));
}

// Implementation of Utility.edit_distance_lev.
static VALUE util_edit_distance_lev(int argc, VALUE* argv, VALUE vself) {
  volatile VALUE vstra, vstrb, vutf;
  rb_scan_args(argc, argv, "21", &vstra, &vstrb, &vutf);
  vstra = StringValueEx(vstra);
  const std::string_view stra = GetStringView(vstra);
  vstrb = StringValueEx(vstrb);
  const std::string_view strb = GetStringView(vstrb);
  const bool utf = RTEST(vutf);
  int32_t rv = 0;
  if (utf) {
    const std::vector<uint32_t> ucsa = tkrzw::ConvertUTF8ToUCS4(stra);
    const std::vector<uint32_t> ucsb = tkrzw::ConvertUTF8ToUCS4(strb);
    rv = tkrzw::EditDistanceLev<std::vector<uint32_t>>(ucsa, ucsb);
  } else {
    rv = tkrzw::EditDistanceLev(stra, strb);
  }
  return INT2FIX(rv);
}

// Defines the Utility class.
static void DefineUtility() {
  cls_util = rb_define_class_under(mod_tkrzw, "Utility", rb_cObject);
  rb_define_const(cls_util, "VERSION", rb_str_new2(tkrzw::PACKAGE_VERSION));
  rb_define_const(cls_util, "INT32MIN", LL2NUM(tkrzw::INT32MIN));
  rb_define_const(cls_util, "INT32MAX", LL2NUM(tkrzw::INT32MAX));
  rb_define_const(cls_util, "UINT32MAX", ULL2NUM(tkrzw::UINT32MAX));
  rb_define_const(cls_util, "INT64MIN", LL2NUM(tkrzw::INT64MIN));
  rb_define_const(cls_util, "INT64MAX", LL2NUM(tkrzw::INT64MAX));
  rb_define_const(cls_util, "UINT64MAX", ULL2NUM(tkrzw::UINT64MAX));
  rb_define_singleton_method(cls_util, "get_memory_usage", (METHOD)util_get_memory_usage, 0);
  rb_define_singleton_method(cls_util, "primary_hash", (METHOD)util_primary_hash, -1);
  rb_define_singleton_method(cls_util, "secondary_hash", (METHOD)util_secondary_hash, -1);
  rb_define_singleton_method(cls_util, "edit_distance_lev", (METHOD)util_edit_distance_lev, -1);
}

// Implementation of Status#del.
static void status_del(void* ptr) {
  delete (tkrzw::Status*)ptr;
}

// Implementation of Status.new.
static VALUE status_new(VALUE cls) {
  tkrzw::Status* status = new tkrzw::Status(tkrzw::Status::SUCCESS);
  return Data_Wrap_Struct(cls_status, 0, status_del, status);
}

// Implementation of Status#initialize.
static VALUE status_initialize(int argc, VALUE* argv, VALUE vself) {
  volatile VALUE vcode, vmessage;
  rb_scan_args(argc, argv, "02", &vcode, &vmessage);
  if (vcode == Qnil) {
    vcode = INT2FIX(tkrzw::Status::SUCCESS);
  }
  if (vmessage == Qnil) {
    vmessage = rb_str_new2("");
  }
  vmessage = StringValueEx(vmessage);
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  status->Set((tkrzw::Status::Code)NUM2INT(vcode), GetStringView(vmessage));
  return Qnil;
}

// Implementation of Status#set.
static VALUE status_set(int argc, VALUE* argv, VALUE vself) {
  volatile VALUE vcode, vmessage;
  rb_scan_args(argc, argv, "02", &vcode, &vmessage);
  if (vcode == Qnil) {
    vcode = INT2FIX(tkrzw::Status::SUCCESS);
  }
  if (vmessage == Qnil) {
    vmessage = rb_str_new2("");
  }
  vmessage = StringValueEx(vmessage);
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  status->Set((tkrzw::Status::Code)NUM2INT(vcode), GetStringView(vmessage));
  return Qnil;
}

// Implementation of Status#code.
static VALUE status_code(VALUE vself) {
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  return INT2FIX(status->GetCode());
}

// Implementation of Status#get_message.
static VALUE status_message(VALUE vself) {
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  const std::string_view message = status->GetMessage();
  return rb_str_new(message.data(), message.size());
}

// Implementation of Status#ok?.
static VALUE status_ok(VALUE vself) {
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  return status->IsOK() ? Qtrue : Qfalse;
}

// Implementation of Status#or_die.
static VALUE status_or_die(VALUE vself) {
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  if (!status->IsOK()) {
    // TODO: The thrown exception should be initialized with the status object.
    const std::string& message = tkrzw::ToString(*status);
    rb_raise(cls_expt, "%s", message.c_str());
  }
  return Qnil;
}

// Implementation of Status#to_s.
static VALUE status_to_s(VALUE vself) {
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  const std::string str = tkrzw::ToString(*status);
  return rb_str_new(str.data(), str.size());
}

// Implementation of Status#inspect.
static VALUE status_inspect(VALUE vself) {
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  const std::string str = tkrzw::StrCat("#<tkrzw::Status:", *status, ">");
  return rb_str_new(str.data(), str.size());
}

// Implementation of Status#op_eq.
static VALUE status_op_eq(VALUE vself, VALUE vrhs) {
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  int32_t rcode = 0;
  if (rb_obj_is_instance_of(vrhs, cls_status)) {
    tkrzw::Status* rstatus = nullptr;
    Data_Get_Struct(vrhs, tkrzw::Status, rstatus);
    rcode = rstatus->GetCode();
  } else if (TYPE(vrhs) == T_FIXNUM) {
    rcode = FIX2INT(vrhs);
  } else {
    return Qfalse;
  }
  return status->GetCode() == rcode ? Qtrue : Qfalse;
}

// Implementation of Status#op_ne.
static VALUE status_op_ne(VALUE vself, VALUE vrhs) {
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  int32_t rcode = 0;
  if (rb_obj_is_instance_of(vrhs, cls_status)) {
    tkrzw::Status* rstatus = nullptr;
    Data_Get_Struct(vrhs, tkrzw::Status, rstatus);
    rcode = rstatus->GetCode();
  } else if (TYPE(vrhs) == T_FIXNUM) {
    rcode = FIX2INT(vrhs);
  } else {
    return Qfalse;
  }
  return status->GetCode() == rcode ? Qfalse : Qtrue;
}

// Creates a status object.
static VALUE MakeStatusValue(tkrzw::Status&& status) {
  tkrzw::Status* new_status = new tkrzw::Status(status);
  return Data_Wrap_Struct(cls_status, 0, status_del, new_status);
}

// Sets a status object.
static void SetStatusValue(VALUE vstatus, const tkrzw::Status& rhs) {
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vstatus, tkrzw::Status, status);
  *status = rhs;
}

// Defines the Status class.
static void DefineStatus() {
  cls_status = rb_define_class_under(mod_tkrzw, "Status", rb_cObject);
  rb_define_alloc_func(cls_status, status_new);
  rb_define_const(cls_status, "SUCCESS",
                  INT2FIX(tkrzw::Status::SUCCESS));
  rb_define_const(cls_status, "UNKNOWN_ERROR",
                  INT2FIX(tkrzw::Status::UNKNOWN_ERROR));
  rb_define_const(cls_status, "SYSTEM_ERROR",
                  INT2FIX(tkrzw::Status::SYSTEM_ERROR));
  rb_define_const(cls_status, "NOT_IMPLEMENTED_ERROR",
                  INT2FIX(tkrzw::Status::NOT_IMPLEMENTED_ERROR));
  rb_define_const(cls_status, "PRECONDITION_ERROR",
                  INT2FIX(tkrzw::Status::PRECONDITION_ERROR));
  rb_define_const(cls_status, "INVALID_ARGUMENT_ERROR",
                  INT2FIX(tkrzw::Status::INVALID_ARGUMENT_ERROR));
  rb_define_const(cls_status, "CANCELED_ERROR",
                  INT2FIX(tkrzw::Status::CANCELED_ERROR));
  rb_define_const(cls_status, "NOT_FOUND_ERROR",
                  INT2FIX(tkrzw::Status::NOT_FOUND_ERROR));
  rb_define_const(cls_status, "PERMISSION_ERROR",
                  INT2FIX(tkrzw::Status::PERMISSION_ERROR));
  rb_define_const(cls_status, "INFEASIBLE_ERROR",
                  INT2FIX(tkrzw::Status::INFEASIBLE_ERROR));
  rb_define_const(cls_status, "DUPLICATION_ERROR",
                  INT2FIX(tkrzw::Status::DUPLICATION_ERROR));
  rb_define_const(cls_status, "BROKEN_DATA_ERROR",
                  INT2FIX(tkrzw::Status::BROKEN_DATA_ERROR));
  rb_define_const(cls_status, "APPLICATION_ERROR",
                  INT2FIX(tkrzw::Status::APPLICATION_ERROR));
  rb_define_private_method(cls_status, "initialize", (METHOD)status_initialize, -1);
  rb_define_method(cls_status, "set", (METHOD)status_set, -1);
  rb_define_method(cls_status, "code", (METHOD)status_code, 0);
  rb_define_method(cls_status, "message", (METHOD)status_message, 0);
  rb_define_method(cls_status, "ok?", (METHOD)status_ok, 0);
  rb_define_method(cls_status, "or_die", (METHOD)status_or_die, 0);
  rb_define_method(cls_status, "to_s", (METHOD)status_to_s, 0);
  rb_define_method(cls_status, "to_i", (METHOD)status_code, 0);
  rb_define_method(cls_status, "inspect", (METHOD)status_inspect, 0);
  rb_define_method(cls_status, "==", (METHOD)status_op_eq, 1);
  rb_define_method(cls_status, "!=", (METHOD)status_op_ne, 1);
}

// Implementation of StatusException#initialize.
static VALUE expt_initialize(VALUE vself, VALUE vstatus) {
  volatile VALUE vmessage = StringValueEx(vstatus);
  volatile VALUE vsuperargs[] = {vmessage};
  rb_call_super(1, (const VALUE*)vsuperargs);
  if (!rb_obj_is_instance_of(vstatus, cls_status)) {
    tkrzw::Status::Code code = tkrzw::Status::SYSTEM_ERROR;
    std::string_view message = GetStringView(vmessage);
    size_t pos = message.find(':');
    if (pos != std::string::npos) {
      const std::string_view name = message.substr(0, pos);
      pos++;
      while (pos < message.size() && message[pos] == ' ') {
        pos++;
      }
      const std::string_view sub_message = message.substr(pos);
      int32_t num = 0;
      while (num <= (int)tkrzw::Status::APPLICATION_ERROR) {
        if (name == tkrzw::Status::CodeName((tkrzw::Status::Code)num)) {
          code = (tkrzw::Status::Code)num;
          message = sub_message;
          break;
        }
        num++;
      }
    }
    vstatus = MakeStatusValue(tkrzw::Status(code, message));
  }
  rb_ivar_set(vself, id_expt_status, vstatus);
  return Qnil;
}

// Implementation of StatusException#status.
static VALUE expt_status(VALUE vself) {
  return rb_ivar_get(vself, id_expt_status);
}

// Defines the StatusException class.
static void DefineStatusException() {
  cls_expt = rb_define_class_under(mod_tkrzw, "StatusException", rb_eRuntimeError);
  rb_define_private_method(cls_expt, "initialize", (METHOD)expt_initialize, 1);
  rb_define_method(cls_expt, "status", (METHOD)expt_status, 0);
  id_expt_status = rb_intern("@status");
}

// Implementation of DBM#del.
static void dbm_del(void* ptr) {
  delete (StructDBM*)ptr;
}

// Implementation of DBM.new.
static VALUE dbm_new(VALUE cls) {
  StructDBM* sdbm = new StructDBM;
  return Data_Wrap_Struct(cls_dbm, 0, dbm_del, sdbm);
}

// Implementation of DBM#initialize.
static VALUE dbm_initialize(VALUE vself) {
  return Qnil;
}

// Implementation of DBM#destruct.
static VALUE dbm_destruct(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  sdbm->dbm.reset(nullptr);
  return Qnil;
}

// Implementation of DBM#open.
static VALUE dbm_open(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm != nullptr) {
    rb_raise(rb_eRuntimeError, "opened database");
  }
  volatile VALUE vpath, vwritable, vparams;
  rb_scan_args(argc, argv, "21", &vpath, &vwritable, &vparams);
  vpath = StringValueEx(vpath);
  const std::string_view path = GetStringView(vpath);
  const bool writable = RTEST(vwritable);
  std::map<std::string, std::string> params = HashToMap(vparams);
  const int32_t num_shards = tkrzw::StrToInt(tkrzw::SearchMap(params, "num_shards", "-1"));
  bool concurrent = false;
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "concurrent", "false"))) {
    concurrent = true;
  }
  int32_t open_options = 0;
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "truncate", "false"))) {
    open_options |= tkrzw::File::OPEN_TRUNCATE;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_create", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_CREATE;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_wait", "false"))) {
      open_options |= tkrzw::File::OPEN_NO_WAIT;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "no_lock", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_LOCK;
  }
  std::string encoding = tkrzw::SearchMap(params, "encoding", "");
  if (encoding.empty()) {
    encoding = "ASCII-8BIT";
  }
  params.erase("concurrent");
  params.erase("truncate");
  params.erase("no_create");
  params.erase("no_wait");
  params.erase("no_lock");
  params.erase("encoding");
  if (num_shards >= 0) {
    sdbm->dbm.reset(new tkrzw::ShardDBM());
  } else {
    sdbm->dbm.reset(new tkrzw::PolyDBM());
  }
  sdbm->concurrent = concurrent;
  sdbm->venc = GetEncoding(encoding);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->OpenAdvanced(std::string(path), writable, open_options, params);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#close.
static VALUE dbm_close(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Close();
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#get.
static VALUE dbm_get(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vkey, vstatus;
  rb_scan_args(argc, argv, "11", &vkey, &vstatus);
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  std::string value;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Get(key, &value);
    });
  if (rb_obj_is_instance_of(vstatus, cls_status)) {
    SetStatusValue(vstatus, status);
  }
  if (status == tkrzw::Status::SUCCESS) {
    return MakeString(value, sdbm->venc);
  }
  return Qnil;
}

// Implementation of DBM#get_multi.
static VALUE dbm_get_multi(VALUE vself, VALUE vkeys) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  if (TYPE(vkeys) != T_ARRAY) {
    rb_raise(rb_eRuntimeError, "keys is not an array");
  }
  std::vector<std::string> keys;
  const int32_t num_keys = RARRAY_LEN(vkeys);
  for (int32_t i = 0; i < num_keys; i++) {
    volatile VALUE vkey = rb_ary_entry(vkeys, i);
    vkey = StringValueEx(vkey);
    keys.emplace_back(std::string(RSTRING_PTR(vkey), RSTRING_LEN(vkey)));
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  std::map<std::string, std::string> records;
  NativeFunction(sdbm->concurrent, [&]() {
      records = sdbm->dbm->GetMulti(key_views);
    });
  volatile VALUE vhash = rb_hash_new();
  for (const auto& record : records) {
    volatile VALUE vkey = rb_str_new(record.first.data(), record.first.size());
    volatile VALUE vvalue = rb_str_new(record.second.data(), record.second.size());
    rb_hash_aset(vhash, vkey, vvalue);
  }
  return vhash;
}

// Implementation of DBM#set.
static VALUE dbm_set(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vkey, vvalue, voverwrite;
  rb_scan_args(argc, argv, "21", &vkey, &vvalue, &voverwrite);
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  vvalue = StringValueEx(vvalue);
  const std::string_view value = GetStringView(vvalue);
  const bool overwrite = argc > 2 ? RTEST(voverwrite) : true;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Set(key, value, overwrite);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#set_multi.
static VALUE dbm_set_multi(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vrecords;
  rb_scan_args(argc, argv, "1", &vrecords);
  const auto& records = HashToMap(vrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->SetMulti(record_views);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#set_and_get.
static VALUE dbm_set_and_get(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vkey, vvalue, voverwrite;
  rb_scan_args(argc, argv, "21", &vkey, &vvalue, &voverwrite);
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  vvalue = StringValueEx(vvalue);
  const std::string_view value = GetStringView(vvalue);
  const bool overwrite = argc > 2 ? RTEST(voverwrite) : true;
  tkrzw::Status impl_status(tkrzw::Status::SUCCESS);
  std::string old_value;
  bool hit = false;
  class Processor final : public tkrzw::DBM::RecordProcessor {
   public:
    Processor(tkrzw::Status* status, std::string_view value, bool overwrite,
              std::string* old_value, bool* hit)
        : status_(status), value_(value), overwrite_(overwrite),
          old_value_(old_value), hit_(hit) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      *old_value_ = value;
      *hit_ = true;
      if (overwrite_) {
        return value_;
      }
      status_->Set(tkrzw::Status::DUPLICATION_ERROR);
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      return value_;
    }
   private:
    tkrzw::Status* status_;
    std::string_view value_;
    bool overwrite_;
    std::string* old_value_;
    bool* hit_;
  };
  Processor proc(&impl_status, value, overwrite, &old_value, &hit);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Process(key, &proc, true);
    });
  status |= impl_status;
  volatile VALUE vpair = rb_ary_new2(2);
  rb_ary_push(vpair, MakeStatusValue(std::move(status)));
  if (hit) {
    rb_ary_push(vpair, MakeString(old_value, sdbm->venc));
  } else {
    rb_ary_push(vpair, Qnil);
  }
  return vpair;
}

// Implementation of DBM#remove.
static VALUE dbm_remove(VALUE vself, VALUE vkey) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Remove(key);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#remove_multi.
static VALUE dbm_remove_multi(VALUE vself, VALUE vkeys) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  if (TYPE(vkeys) != T_ARRAY) {
    rb_raise(rb_eRuntimeError, "keys is not an array");
  }
  std::vector<std::string> keys;
  const int32_t num_keys = RARRAY_LEN(vkeys);
  for (int32_t i = 0; i < num_keys; i++) {
    volatile VALUE vkey = rb_ary_entry(vkeys, i);
    vkey = StringValueEx(vkey);
    keys.emplace_back(std::string(RSTRING_PTR(vkey), RSTRING_LEN(vkey)));
  }
  std::vector<std::string_view> key_views(keys.begin(), keys.end());
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->RemoveMulti(key_views);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#remove_and_get.
static VALUE dbm_remove_and_get(VALUE vself, VALUE vkey) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  tkrzw::Status impl_status(tkrzw::Status::SUCCESS);
  std::string old_value;
  class Processor final : public tkrzw::DBM::RecordProcessor {
   public:
    Processor(tkrzw::Status* status, std::string* old_value)
        : status_(status), old_value_(old_value) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      *old_value_ = value;
      return REMOVE;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      status_->Set(tkrzw::Status::NOT_FOUND_ERROR);
      return NOOP;
    }
   private:
    tkrzw::Status* status_;
    std::string* old_value_;
  };
  Processor proc(&impl_status, &old_value);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Process(key, &proc, true);
    });
  status |= impl_status;
  volatile VALUE vpair = rb_ary_new2(2);
  rb_ary_push(vpair, MakeStatusValue(std::move(status)));
  if (status == tkrzw::Status::SUCCESS) {
    rb_ary_push(vpair, MakeString(old_value, sdbm->venc));
  } else {
    rb_ary_push(vpair, Qnil);
  }
  return vpair;
}

// Implementation of DBM#append.
static VALUE dbm_append(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vkey, vvalue, vdelim;
  rb_scan_args(argc, argv, "21", &vkey, &vvalue, &vdelim);
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  vvalue = StringValueEx(vvalue);
  const std::string_view value = GetStringView(vvalue);
  vdelim = StringValueEx(vdelim);
  const std::string_view delim = GetStringView(vdelim);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Append(key, value, delim);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#compare_exchange.
static VALUE dbm_compare_exchange(VALUE vself, VALUE vkey, VALUE vexpected, VALUE vdesired) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  std::string_view expected;
  if (vexpected != Qnil) {
    vexpected = StringValueEx(vexpected);
    expected = GetStringView(vexpected);
  }
  std::string_view desired;
  if (vdesired != Qnil) {
    vdesired = StringValueEx(vdesired);
    desired = GetStringView(vdesired);
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->CompareExchange(key, expected, desired);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#increment.
static VALUE dbm_increment(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vkey, vinc, vinit, vstatus;
  rb_scan_args(argc, argv, "13", &vkey, &vinc, &vinit, &vstatus);
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  const int64_t inc = vinc == Qnil ? 1 : GetInteger(vinc);
  const int64_t init = vinit == Qnil ? 0 : GetInteger(vinit);
  int64_t current = 0;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Increment(key, inc, &current, init);
    });
  if (rb_obj_is_instance_of(vstatus, cls_status)) {
    SetStatusValue(vstatus, status);
  }
  if (status == tkrzw::Status::SUCCESS) {
    return LONG2NUM(current);
  }
  return Qnil;
}

// Implementation of DBM#compare_exchange_multi.
static VALUE dbm_compare_exchange_multi(VALUE vself, VALUE vexpected, VALUE vdesired) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  if (TYPE(vexpected) != T_ARRAY || TYPE(vdesired) != T_ARRAY) {
    rb_raise(rb_eRuntimeError, "expected or desired is not an array");
  }
  const auto& expected = ExtractSVPairs(vexpected);
  const auto& desired = ExtractSVPairs(vdesired);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->CompareExchangeMulti(expected, desired);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#count.
static VALUE dbm_count(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  int64_t count = 0;
  NativeFunction(sdbm->concurrent, [&]() {
      count = sdbm->dbm->CountSimple();
    });
  if (count >= 0) {
    return LONG2NUM(count);
  }
  return Qnil;
}

// Implementation of DBM#file_size.
static VALUE dbm_file_size(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  int64_t file_size = 0;
  NativeFunction(sdbm->concurrent, [&]() {
      file_size = sdbm->dbm->GetFileSizeSimple();
    });
  if (file_size >= 0) {
    return LONG2NUM(file_size);
  }
  return Qnil;
}

// Implementation of DBM#path.
static VALUE dbm_path(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  std::string path;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->GetFilePath(&path);
    });
  if (status == tkrzw::Status::SUCCESS) {
    return rb_str_new(path.data(), path.size());
  }
  return Qnil;
}

// Implementation of DBM#clear.
static VALUE dbm_clear(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Clear();
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#rebuild.
static VALUE dbm_rebuild(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vparams;
  rb_scan_args(argc, argv, "01", &vparams);
  const std::map<std::string, std::string> params = HashToMap(vparams);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->RebuildAdvanced(params);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#should_be_rebuilt?.
static VALUE dbm_should_be_rebuilt(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  bool tobe = false;
  NativeFunction(sdbm->concurrent, [&]() {
      tobe = sdbm->dbm->ShouldBeRebuiltSimple();
    });
  return tobe ? Qtrue : Qfalse;
}

// Implementation of DBM#synchronize.
static VALUE dbm_synchronize(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vhard, vparams;
  rb_scan_args(argc, argv, "11", &vhard, &vparams);
  const bool hard = RTEST(vhard);
  const std::map<std::string, std::string> params = HashToMap(vparams);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->SynchronizeAdvanced(hard, nullptr, params);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#copy_file_data.
static VALUE dbm_copy_file_data(VALUE vself, VALUE vdestpath) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  vdestpath = StringValueEx(vdestpath);
  const std::string_view dest_path = GetStringView(vdestpath);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->CopyFileData(std::string(dest_path));
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#export.
static VALUE dbm_export(VALUE vself, VALUE vdestdbm) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  if (!rb_obj_is_instance_of(vdestdbm, cls_dbm)) {
    return MakeStatusValue(tkrzw::Status(tkrzw::Status::INVALID_ARGUMENT_ERROR));
  }
  StructDBM* dest_sdbm = nullptr;
  Data_Get_Struct(vdestdbm, StructDBM, dest_sdbm);
  if (dest_sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Export(dest_sdbm->dbm.get());
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#export_keys_as_lines.
static VALUE dbm_export_keys_as_lines(VALUE vself, VALUE vdestpath) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  vdestpath = StringValueEx(vdestpath);
  const std::string_view dest_path = GetStringView(vdestpath);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      tkrzw::MemoryMapParallelFile file;
      status = file.Open(std::string(dest_path), true, tkrzw::File::OPEN_TRUNCATE);
      if (status == tkrzw::Status::SUCCESS) {
        status |= tkrzw::ExportDBMKeysAsLines(sdbm->dbm.get(), &file);
        status |= file.Close();
      }
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#inspect_details.
static VALUE dbm_inspect_details(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  std::vector<std::pair<std::string, std::string>> records;
  NativeFunction(sdbm->concurrent, [&]() {
      records = sdbm->dbm->Inspect();
    });
  volatile VALUE vhash = rb_hash_new();
  for (const auto& record : records) {
    volatile VALUE vkey = rb_str_new(record.first.data(), record.first.size());
    volatile VALUE vvalue = rb_str_new(record.second.data(), record.second.size());
    rb_hash_aset(vhash, vkey, vvalue);
  }
  return vhash;
}

// Implementation of DBM#open?.
static VALUE dbm_is_open(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  return sdbm->dbm == nullptr ? Qfalse : Qtrue;
}

// Implementation of DBM#healthy?.
static VALUE dbm_is_healthy(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  bool healthy = false;
  NativeFunction(sdbm->concurrent, [&]() {
      healthy = sdbm->dbm->IsHealthy();
    });
  return healthy ? Qtrue : Qfalse;
}

// Implementation of DBM#ordered?.
static VALUE dbm_is_ordered(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  bool ordered = false;
  NativeFunction(sdbm->concurrent, [&]() {
      ordered = sdbm->dbm->IsOrdered();
    });
  return ordered ? Qtrue : Qfalse;
}

// Implementation of DBM#search.
static VALUE dbm_search(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vmode, vpattern, vcapacity, vutf;
  rb_scan_args(argc, argv, "22", &vmode, &vpattern, &vcapacity, &vutf);
  vmode = StringValueEx(vmode);
  const std::string_view mode = GetStringView(vmode);
  vpattern = StringValueEx(vpattern);
  const std::string_view pattern = GetStringView(vpattern);
  const int64_t capacity = GetInteger(vcapacity);
  const bool utf = RTEST(vutf);
  std::vector<std::string> keys;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
    status = tkrzw::SearchDBMModal(
        sdbm->dbm.get(), mode, pattern, &keys, capacity, utf);
  });
  if (status != tkrzw::Status::SUCCESS) {
    const std::string& message = tkrzw::ToString(status);
    rb_raise(cls_expt, "%s", message.c_str());
  }
  volatile VALUE vkeys = rb_ary_new2(keys.size());
  for (const auto& key : keys) {
    rb_ary_push(vkeys, MakeString(key, sdbm->venc));
  }
  return vkeys;
}

// Implementation of DBM#make_iterator.
static VALUE dbm_make_iterator(VALUE vself) {
  return rb_class_new_instance(1, &vself, cls_iter);
}

// Implementation of DBM.restore_database.
static VALUE dbm_restore_database(int argc, VALUE* argv, VALUE vself) {
  volatile VALUE vold_file_path, vnew_file_path, vclass_name, vend_offset;
  rb_scan_args(argc, argv, "22", &vold_file_path, &vnew_file_path, &vclass_name, &vend_offset);
  const std::string_view old_file_path = GetStringView(StringValueEx(vold_file_path));
  const std::string_view new_file_path = GetStringView(StringValueEx(vnew_file_path));
  const std::string_view class_name = GetStringView(StringValueEx(vclass_name));
  const int64_t end_offset = vend_offset == Qnil ? -1 : GetInteger(vend_offset);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  int32_t num_shards = 0;
  if (tkrzw::ShardDBM::GetNumberOfShards(std::string(old_file_path), &num_shards) ==
      tkrzw::Status::SUCCESS) {
    NativeFunction(true, [&]() {
      status = tkrzw::ShardDBM::RestoreDatabase(
        std::string(old_file_path), std::string(new_file_path),
        std::string(class_name), end_offset);
    });
  } else {
    NativeFunction(true, [&]() {
      status = tkrzw::PolyDBM::RestoreDatabase(
          std::string(old_file_path), std::string(new_file_path),
          std::string(class_name), end_offset);
    });
  }
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#to_s.
static VALUE dbm_to_s(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }  
  std::string class_name = "unknown";
  std::string path = "-";
  int64_t count = -1;
  NativeFunction(sdbm->concurrent, [&]() {
      for (const auto& rec : sdbm->dbm->Inspect()) {
        if (rec.first == "class") {
          class_name = rec.second;
        } else if (rec.first == "path") {
          path = rec.second;
        }
      }
      count = sdbm->dbm->CountSimple();
    });
  const std::string expr =
      tkrzw::StrCat(class_name, ":", tkrzw::StrEscapeC(path, true), ":", count);
  return rb_str_new(expr.data(), expr.size());
}

// Implementation of DBM#to_i.
static VALUE dbm_to_i(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  int64_t count = -1;
  NativeFunction(sdbm->concurrent, [&]() {
      count = sdbm->dbm->CountSimple();
    });
  return LL2NUM(count);
}

// Implementation of DBM#inspect.
static VALUE dbm_inspect(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  std::string class_name = "unknown";
  std::string path = "-";
  int64_t count = -1;
  NativeFunction(sdbm->concurrent, [&]() {
      for (const auto& rec : sdbm->dbm->Inspect()) {
        if (rec.first == "class") {
          class_name = rec.second;
        } else if (rec.first == "path") {
          path = rec.second;
        }
      }
      count = sdbm->dbm->CountSimple();
    });
  const std::string expr = tkrzw::StrCat(
      "#<tkrzw::DBM:", class_name, ":", tkrzw::StrEscapeC(path, true), ":", count, ">");
  return rb_str_new(expr.data(), expr.size());
}

// Implementation of DBM#[].
static VALUE dbm_ss_get(VALUE vself, VALUE vkey) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  std::string value;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Get(key, &value);
    });
  if (status == tkrzw::Status::SUCCESS) {
    return MakeString(value, sdbm->venc);
  }
  return Qnil;
}

// Implementation of DBM#[]=.
static VALUE dbm_ss_set(VALUE vself, VALUE vkey, VALUE vvalue) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  vvalue = StringValueEx(vvalue);
  const std::string_view value = GetStringView(vvalue);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Set(key, value);
    });
  return vvalue;
}

// Implementation of DBM#each.
static VALUE dbm_each(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  if (!rb_block_given_p()) {
    rb_raise(rb_eArgError, "block is not given");
  }
  std::unique_ptr<tkrzw::DBM::Iterator> iter;
  NativeFunction(sdbm->concurrent, [&]() {
      iter = sdbm->dbm->MakeIterator();
      iter->First();
    });
  while (true) {
    std::string key, value;
    tkrzw::Status status(tkrzw::Status::SUCCESS);
    NativeFunction(sdbm->concurrent, [&]() {
        status = iter->Get(&key, &value);
      });
    if (status != tkrzw::Status::SUCCESS) {
      break;
    }
    volatile VALUE args =
        rb_ary_new3(2, MakeString(key, sdbm->venc), MakeString(value, sdbm->venc));
    int result = 0;
    rb_protect(YieldToBlock, args, &result);
    if (result != 0) {
      rb_jump_tag(result);
      break;
    }
    NativeFunction(sdbm->concurrent, [&]() {
        iter->Next();
      });
  }
  return Qnil;
}

// Defines the DBM class.
static void DefineDBM() {
  cls_dbm = rb_define_class_under(mod_tkrzw, "DBM", rb_cObject);
  rb_define_alloc_func(cls_dbm, dbm_new);
  rb_define_private_method(cls_dbm, "initialize", (METHOD)dbm_initialize, 0);
  rb_define_method(cls_dbm, "destruct", (METHOD)dbm_destruct, 0);
  rb_define_method(cls_dbm, "open", (METHOD)dbm_open, -1);
  rb_define_method(cls_dbm, "close", (METHOD)dbm_close, 0);
  rb_define_method(cls_dbm, "get", (METHOD)dbm_get, -1);
  rb_define_method(cls_dbm, "get_multi", (METHOD)dbm_get_multi, -2);
  rb_define_method(cls_dbm, "set", (METHOD)dbm_set, -1);
  rb_define_method(cls_dbm, "set_multi", (METHOD)dbm_set_multi, -1);
  rb_define_method(cls_dbm, "set_and_get", (METHOD)dbm_set_and_get, -1);
  rb_define_method(cls_dbm, "remove", (METHOD)dbm_remove, 1);
  rb_define_method(cls_dbm, "remove_multi", (METHOD)dbm_remove_multi, -2);
  rb_define_method(cls_dbm, "remove_and_get", (METHOD)dbm_remove_and_get, 1);
  rb_define_method(cls_dbm, "append", (METHOD)dbm_append, -1);
  rb_define_method(cls_dbm, "compare_exchange", (METHOD)dbm_compare_exchange, 3);
  rb_define_method(cls_dbm, "increment", (METHOD)dbm_increment, -1);
  rb_define_method(cls_dbm, "compare_exchange_multi", (METHOD)dbm_compare_exchange_multi, 2);
  rb_define_method(cls_dbm, "count", (METHOD)dbm_count, 0);
  rb_define_method(cls_dbm, "file_size", (METHOD)dbm_file_size, 0);
  rb_define_method(cls_dbm, "path", (METHOD)dbm_path, 0);
  rb_define_method(cls_dbm, "clear", (METHOD)dbm_clear, 0);
  rb_define_method(cls_dbm, "rebuild", (METHOD)dbm_rebuild, -1);
  rb_define_method(cls_dbm, "should_be_rebuilt?", (METHOD)dbm_should_be_rebuilt, 0);
  rb_define_method(cls_dbm, "synchronize", (METHOD)dbm_synchronize, -1);
  rb_define_method(cls_dbm, "copy_file_data", (METHOD)dbm_copy_file_data, 1);
  rb_define_method(cls_dbm, "export", (METHOD)dbm_export, 1);
  rb_define_method(cls_dbm, "export_keys_as_lines", (METHOD)dbm_export_keys_as_lines, 1);
  rb_define_method(cls_dbm, "inspect_details", (METHOD)dbm_inspect_details, 0);
  rb_define_method(cls_dbm, "open?", (METHOD)dbm_is_open, 0);
  rb_define_method(cls_dbm, "healthy?", (METHOD)dbm_is_healthy, 0);
  rb_define_method(cls_dbm, "ordered?", (METHOD)dbm_is_ordered, 0);
  rb_define_method(cls_dbm, "search", (METHOD)dbm_search, -1);
  rb_define_method(cls_dbm, "make_iterator", (METHOD)dbm_make_iterator, 0);
  rb_define_singleton_method(cls_dbm, "restore_database", (METHOD)dbm_restore_database, -1);
  rb_define_method(cls_dbm, "to_s", (METHOD)dbm_to_s, 0);
  rb_define_method(cls_dbm, "to_i", (METHOD)dbm_to_i, 0);
  rb_define_method(cls_dbm, "inspect", (METHOD)dbm_inspect, 0);
  rb_define_method(cls_dbm, "[]", (METHOD)dbm_ss_get, 1);
  rb_define_method(cls_dbm, "[]=", (METHOD)dbm_ss_set, 2);
  rb_define_method(cls_dbm, "each", (METHOD)dbm_each, 0);
}

// Implementation of Iterator#del.
static void iter_del(void* ptr) {
  delete (StructIter*)ptr;
}

// Implementation of Iterator.new.
static VALUE iter_new(VALUE cls) {
  StructIter* siter = new StructIter;
  return Data_Wrap_Struct(cls_iter, 0, iter_del, siter);
}

// Implementation of Iterator#initialize.
static VALUE iter_initialize(VALUE vself, VALUE vdbm) {
  if (!rb_obj_is_instance_of(vdbm, cls_dbm)) {
    rb_raise(rb_eArgError, "#<tkrzw::StatusException>");
  }
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vdbm, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  siter->iter = sdbm->dbm->MakeIterator();
  siter->concurrent = sdbm->concurrent;
  siter->venc = sdbm->venc;
  return Qnil;
}

// Implementation of Iterator#destruct.
static VALUE iter_destruct(VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  siter->iter.reset(nullptr);
  return Qnil;
}

// Implementation of Iterator#first.
static VALUE iter_first(VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->First();
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of Iterator#last.
static VALUE iter_last(VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Last();
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of Iterator#jump.
static VALUE iter_jump(VALUE vself, VALUE vkey) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Jump(key);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of Iterator#jump_lower.
static VALUE iter_jump_lower(int argc, VALUE* argv, VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  volatile VALUE vkey, vinclusive;
  rb_scan_args(argc, argv, "11", &vkey, &vinclusive);
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  const bool inclusive = argc > 1 ? RTEST(vinclusive) :false;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->JumpLower(key, inclusive);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of Iterator#jump_upper.
static VALUE iter_jump_upper(int argc, VALUE* argv, VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  volatile VALUE vkey, vinclusive;
  rb_scan_args(argc, argv, "11", &vkey, &vinclusive);
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  const bool inclusive = argc > 1 ? RTEST(vinclusive) :false;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->JumpUpper(key, inclusive);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of Iterator#next.
static VALUE iter_next(VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Next();
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of Iterator#previous.
static VALUE iter_previous(VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Previous();
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of Iterator#get.
static VALUE iter_get(int argc, VALUE* argv, VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  volatile VALUE vstatus;
  rb_scan_args(argc, argv, "01", &vstatus);
  std::string key, value;
   tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Get(&key, &value);
    });
  if (rb_obj_is_instance_of(vstatus, cls_status)) {
    SetStatusValue(vstatus, status);
  }
  if (status == tkrzw::Status::SUCCESS) {
    volatile VALUE vary = rb_ary_new2(2);
    rb_ary_push(vary, MakeString(key, siter->venc));
    rb_ary_push(vary, MakeString(value, siter->venc));
    return vary;
  }
  return Qnil;
}

// Implementation of Iterator#get_key.
static VALUE iter_get_key(int argc, VALUE* argv, VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  volatile VALUE vstatus;
  rb_scan_args(argc, argv, "01", &vstatus);
  std::string key;
   tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Get(&key);
    });
  if (rb_obj_is_instance_of(vstatus, cls_status)) {
    SetStatusValue(vstatus, status);
  }
  if (status == tkrzw::Status::SUCCESS) {
    return MakeString(key, siter->venc);
  }
  return Qnil;
}

// Implementation of Iterator#get_value.
static VALUE iter_get_value(int argc, VALUE* argv, VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  volatile VALUE vstatus;
  rb_scan_args(argc, argv, "01", &vstatus);
  std::string value;
   tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Get(nullptr, &value);
    });
  if (rb_obj_is_instance_of(vstatus, cls_status)) {
    SetStatusValue(vstatus, status);
  }
  if (status == tkrzw::Status::SUCCESS) {
    return MakeString(value, siter->venc);
  }
  return Qnil;
}

// Implementation of Iterator#set.
static VALUE iter_set(VALUE vself, VALUE vvalue) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  vvalue = StringValueEx(vvalue);
  const std::string_view value = GetStringView(vvalue);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Set(value);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of Iterator#remove.
static VALUE iter_remove(VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Remove();
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of Iterator#to_s.
static VALUE iter_to_s(VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  std::string key;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Get(&key);
    });
  if (status  != tkrzw::Status::SUCCESS) {
    key = "(unlocated)";
  }
  const std::string& expr = tkrzw::StrEscapeC(key, true);
  return rb_str_new(expr.data(), expr.size());
}

// Implementation of Iterator#inspect.
static VALUE iter_inspect(VALUE vself) {
  StructIter* siter = nullptr;
  Data_Get_Struct(vself, StructIter, siter);
  if (siter->iter == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed Iterator");
  }
  std::string key;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(siter->concurrent, [&]() {
      status = siter->iter->Get(&key);
    });
  if (status  != tkrzw::Status::SUCCESS) {
    key = "(unlocated)";
  }
  const std::string& expr =
      tkrzw::StrCat("#<tkrz::Iterator:", tkrzw::StrEscapeC(key, true), ">");
  return rb_str_new(expr.data(), expr.size());
}

// Defines the Iterator class.
static void DefineIterator() {
  cls_iter = rb_define_class_under(mod_tkrzw, "Iterator", rb_cObject);
  rb_define_alloc_func(cls_iter, iter_new);
  rb_define_private_method(cls_iter, "initialize", (METHOD)iter_initialize, 1);
  rb_define_method(cls_iter, "destruct", (METHOD)iter_destruct, 0);
  rb_define_method(cls_iter, "first", (METHOD)iter_first, 0);
  rb_define_method(cls_iter, "last", (METHOD)iter_last, 0);
  rb_define_method(cls_iter, "jump", (METHOD)iter_jump, 1);
  rb_define_method(cls_iter, "jump_lower", (METHOD)iter_jump_lower, -1);
  rb_define_method(cls_iter, "jump_upper", (METHOD)iter_jump_upper, -1);
  rb_define_method(cls_iter, "next", (METHOD)iter_next, 0);
  rb_define_method(cls_iter, "previous", (METHOD)iter_previous, 0);
  rb_define_method(cls_iter, "get", (METHOD)iter_get, -1);
  rb_define_method(cls_iter, "get_key", (METHOD)iter_get_key, -1);
  rb_define_method(cls_iter, "get_value", (METHOD)iter_get_value, -1);
  rb_define_method(cls_iter, "set", (METHOD)iter_set, 1);
  rb_define_method(cls_iter, "remove", (METHOD)iter_remove, 0);
  rb_define_method(cls_iter, "to_s", (METHOD)iter_to_s, 0);
  rb_define_method(cls_iter, "inspect", (METHOD)iter_inspect, 0);
}

// Implementation of TextFile#del.
static void textfile_del(void* ptr) {
  delete (StructTextFile*)ptr;
}

// Implementation of TextFile.new.
static VALUE textfile_new(VALUE cls) {
  StructTextFile* sfile = new StructTextFile(new tkrzw::MemoryMapParallelFile);
  return Data_Wrap_Struct(cls_textfile, 0, textfile_del, sfile);
}

// Implementation of TextFile#initialize.
static VALUE textfile_initialize(VALUE vself) {
  return Qnil;
}

// Implementation of TextFile#destruct.
static VALUE textfile_destruct(VALUE vself) {
  StructTextFile* sfile = nullptr;
  Data_Get_Struct(vself, StructTextFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed TextFile");
  }
  sfile->file.reset(nullptr);
  return Qnil;
}

// Implementation of TextFile#open.
static VALUE textfile_open(int argc, VALUE* argv, VALUE vself) {
  StructTextFile* sfile = nullptr;
  Data_Get_Struct(vself, StructTextFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed TextFile");
  }
  volatile VALUE vpath, vparams;
  rb_scan_args(argc, argv, "11", &vpath, &vparams);
  vpath = StringValueEx(vpath);
  const std::string_view path = GetStringView(vpath);
  std::map<std::string, std::string> params = HashToMap(vparams);
  bool concurrent = false;
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "concurrent", "false"))) {
    concurrent = true;
  }
  std::string encoding = tkrzw::SearchMap(params, "encoding", "");
  if (encoding.empty()) {
    encoding = "ASCII-8BIT";
  }
  sfile->concurrent = concurrent;
  sfile->venc = GetEncoding(encoding);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sfile->concurrent, [&]() {
      status = sfile->file->Open(std::string(path), false);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of TextFile#close.
static VALUE textfile_close(VALUE vself) {
  StructTextFile* sfile = nullptr;
  Data_Get_Struct(vself, StructTextFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed TextFile");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(true, [&]() {
      status = sfile->file->Close();
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of TextFile#search.
static VALUE textfile_search(int argc, VALUE* argv, VALUE vself) {
  StructTextFile* sfile = nullptr;
  Data_Get_Struct(vself, StructTextFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed TextFile");
  }
  volatile VALUE vmode, vpattern, vcapacity, vutf;
  rb_scan_args(argc, argv, "22", &vmode, &vpattern, &vcapacity, &vutf);
  vmode = StringValueEx(vmode);
  const std::string_view mode = GetStringView(vmode);
  vpattern = StringValueEx(vpattern);
  const std::string_view pattern = GetStringView(vpattern);
  const int64_t capacity = GetInteger(vcapacity);
  const bool utf = RTEST(vutf);
  std::vector<std::string> lines;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sfile->concurrent, [&]() {
    status = tkrzw::SearchTextFileModal(
        sfile->file.get(), mode, pattern, &lines, capacity, utf);
  });
  if (status != tkrzw::Status::SUCCESS) {
    const std::string& message = tkrzw::ToString(status);
    rb_raise(cls_expt, "%s", message.c_str());
  }
  volatile VALUE vlines = rb_ary_new2(lines.size());
  for (const auto& line : lines) {
    rb_ary_push(vlines, MakeString(line, sfile->venc));
  }
  return vlines;
}

// Implementation of TextFile#to_s.
static VALUE textfile_to_s(VALUE vself) {
  char kbuf[64];
  std::sprintf(kbuf, "TextFile:0x%llx", (long long)rb_obj_id(vself));
  return rb_str_new2(kbuf);
}

// Implementation of TextFile#inspect.
static VALUE textfile_inspect(VALUE vself) {
  char kbuf[64];
  std::sprintf(kbuf, "#<TextFile:0x%llx>", (long long)rb_obj_id(vself));
  return rb_str_new2(kbuf);
}


// Defines the TextFile class.
static void DefineTextFile() {
  cls_textfile = rb_define_class_under(mod_tkrzw, "TextFile", rb_cObject);
  rb_define_alloc_func(cls_textfile, textfile_new);
  rb_define_private_method(cls_textfile, "initialize", (METHOD)textfile_initialize, 0);
  rb_define_method(cls_textfile, "destruct", (METHOD)textfile_destruct, 0);
  rb_define_method(cls_textfile, "open", (METHOD)textfile_open, -1);
  rb_define_method(cls_textfile, "close", (METHOD)textfile_close, 0);
  rb_define_method(cls_textfile, "search", (METHOD)textfile_search, -1);
  rb_define_method(cls_textfile, "to_s", (METHOD)textfile_to_s, 0);
  rb_define_method(cls_textfile, "inspect", (METHOD)textfile_inspect, 0);
}

// Entry point of the library.
void Init_tkrzw() {
  DefineModule();
  DefineUtility();
  DefineStatus();
  DefineStatusException();
  DefineDBM();
  DefineIterator();
  DefineTextFile();
}

}  // extern "C"

// END OF FILE
