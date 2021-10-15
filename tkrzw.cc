/*************************************************************************************************
 * Ruby binding of Tkrzw
 *
 * Copyright 2020 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
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
#include "tkrzw_file_poly.h"
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
ID id_obj_to_f;
ID id_str_force_encoding;

VALUE cls_util;
VALUE cls_status;
VALUE cls_future;
VALUE cls_expt;
ID id_expt_status;
VALUE cls_dbm;
VALUE cls_iter;
VALUE cls_asyncdbm;
VALUE cls_file;

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
  return 0;
}

// Gets an real number value of a Ruby object. */
static double GetFloat(VALUE vobj) {
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
  if (rb_respond_to(vobj, id_obj_to_f)) {
    return rb_funcall(vobj, id_obj_to_f, 0);
  }
  return 0;
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
  id_obj_to_f = rb_intern("to_f");
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

// Ruby wrapper of the Future object.
struct StructFuture {
  std::unique_ptr<tkrzw::StatusFuture> future;
  bool concurrent = false;
  volatile VALUE venc = Qnil;
};

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

// Ruby wrapper of the AsyncDBM object.
struct StructAsyncDBM {
  std::unique_ptr<tkrzw::AsyncDBM> async;
  bool concurrent = false;
  volatile VALUE venc = Qnil;
};

// Ruby wrapper of the File object.
struct StructFile {
  std::unique_ptr<tkrzw::PolyFile> file;
  bool concurrent = false;
  volatile VALUE venc = Qnil;
  explicit StructFile(tkrzw::PolyFile* file) : file(file) {}
};

// Implementation of Utility.get_memory_capacity.
static VALUE util_get_memory_capacity(VALUE vself) {
  return LL2NUM(tkrzw::GetMemoryCapacity());
}

// Implementation of Utility.get_memory_usage.
static VALUE util_get_memory_usage(VALUE vself) {
  return LL2NUM(tkrzw::GetMemoryUsage());
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
  rb_define_const(cls_util, "OS_NAME", rb_str_new2(tkrzw::OS_NAME));
  rb_define_const(cls_util, "PAGE_SIZE", INT2FIX(tkrzw::PAGE_SIZE));
  rb_define_const(cls_util, "INT32MIN", LL2NUM(tkrzw::INT32MIN));
  rb_define_const(cls_util, "INT32MAX", LL2NUM(tkrzw::INT32MAX));
  rb_define_const(cls_util, "UINT32MAX", ULL2NUM(tkrzw::UINT32MAX));
  rb_define_const(cls_util, "INT64MIN", LL2NUM(tkrzw::INT64MIN));
  rb_define_const(cls_util, "INT64MAX", LL2NUM(tkrzw::INT64MAX));
  rb_define_const(cls_util, "UINT64MAX", ULL2NUM(tkrzw::UINT64MAX));
  rb_define_singleton_method(cls_util, "get_memory_capacity",
                             (METHOD)util_get_memory_capacity, 0);
  rb_define_singleton_method(cls_util, "get_memory_usage",
                             (METHOD)util_get_memory_usage, 0);
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

// Implementation of Status#join.
static VALUE status_join(VALUE vself, VALUE vrht) {
  tkrzw::Status* status = nullptr;
  Data_Get_Struct(vself, tkrzw::Status, status);
  if (!rb_obj_is_instance_of(vrht, cls_status)) {
    rb_raise(rb_eRuntimeError, "not a status");
  }
  tkrzw::Status* rht = nullptr;
  Data_Get_Struct(vrht, tkrzw::Status, rht);
  *status |= *rht;
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
  const std::string str = tkrzw::StrCat("#<Tkrzw::Status:", *status, ">");
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

// Implementation of Status.code_name.
static VALUE status_code_name(VALUE vself, VALUE vcode) {
  return rb_str_new2(tkrzw::Status::CodeName((tkrzw::Status::Code)NUM2INT(vcode)));
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
  rb_define_const(cls_status, "NETWORK_ERROR",
                  INT2FIX(tkrzw::Status::NETWORK_ERROR));
  rb_define_const(cls_status, "APPLICATION_ERROR",
                  INT2FIX(tkrzw::Status::APPLICATION_ERROR));
  rb_define_private_method(cls_status, "initialize", (METHOD)status_initialize, -1);
  rb_define_method(cls_status, "set", (METHOD)status_set, -1);
  rb_define_method(cls_status, "join", (METHOD)status_join, 1);
  rb_define_method(cls_status, "code", (METHOD)status_code, 0);
  rb_define_method(cls_status, "message", (METHOD)status_message, 0);
  rb_define_method(cls_status, "ok?", (METHOD)status_ok, 0);
  rb_define_method(cls_status, "or_die", (METHOD)status_or_die, 0);
  rb_define_method(cls_status, "to_s", (METHOD)status_to_s, 0);
  rb_define_method(cls_status, "to_i", (METHOD)status_code, 0);
  rb_define_method(cls_status, "inspect", (METHOD)status_inspect, 0);
  rb_define_method(cls_status, "==", (METHOD)status_op_eq, 1);
  rb_define_method(cls_status, "!=", (METHOD)status_op_ne, 1);
  rb_define_singleton_method(cls_status, "code_name", (METHOD)status_code_name, 1);
}

// Implementation of Future#del.
static void future_del(void* ptr) {
  StructFuture* sfuture = (StructFuture*)ptr;
  sfuture->future.reset(nullptr);
  delete sfuture;
}

// Implementation of Future.new.
static VALUE future_new(VALUE cls) {
  StructFuture* sfuture = new StructFuture;
  return Data_Wrap_Struct(cls_future, 0, future_del, sfuture);
}

// Implementation of Future#initialize.
static VALUE future_initialize(int argc, VALUE* argv, VALUE vself) {
  rb_raise(rb_eRuntimeError, "Future cannot be created itself");
  return Qnil;
}

// Implementation of Future#destruct.
static VALUE future_destruct(VALUE vself) {
  StructFuture* sfuture = nullptr;
  Data_Get_Struct(vself, StructFuture, sfuture);
  sfuture->future.reset(nullptr);
  return Qnil;
}

// Implementation of Future#wait.
static VALUE future_wait(int argc, VALUE* argv, VALUE vself) {
  StructFuture* sfuture = nullptr;
  Data_Get_Struct(vself, StructFuture, sfuture);
  if (sfuture->future == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  volatile VALUE vtimeout;
  rb_scan_args(argc, argv, "01", &vtimeout);
  const double timeout = vtimeout == Qnil ? -1.0 : GetFloat(vtimeout);
  bool ok = false;
  NativeFunction(sfuture->concurrent, [&]() {
      ok = sfuture->future->Wait(timeout);
    });
  if (ok) {
    sfuture->concurrent = false;
    return Qtrue;
  }
  return Qfalse;
}

// Implementation of Future#get.
static VALUE future_get(VALUE vself) {
  StructFuture* sfuture = nullptr;
  Data_Get_Struct(vself, StructFuture, sfuture);
  if (sfuture->future == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  const auto& type = sfuture->future->GetExtraType();
  if (type == typeid(tkrzw::Status)) {
    tkrzw::Status status(tkrzw::Status::SUCCESS);
    NativeFunction(sfuture->concurrent, [&]() {
        status = sfuture->future->Get();
      });
    sfuture->future.reset(nullptr);
    return MakeStatusValue(std::move(status));
  }
  if (type == typeid(std::pair<tkrzw::Status, std::string>)) {
    std::pair<tkrzw::Status, std::string> result;
    NativeFunction(sfuture->concurrent, [&]() {
        result = sfuture->future->GetString();
      });
    sfuture->future.reset(nullptr);
    volatile VALUE vpair = rb_ary_new2(2);
    rb_ary_push(vpair, MakeStatusValue(std::move(result.first)));
    rb_ary_push(vpair, MakeString(result.second, sfuture->venc));
    return vpair;
  }
  if (type == typeid(std::pair<tkrzw::Status, std::vector<std::string>>)) {
    std::pair<tkrzw::Status, std::vector<std::string>> result;
    NativeFunction(sfuture->concurrent, [&]() {
        result = sfuture->future->GetStringVector();
      });
    sfuture->future.reset(nullptr);
    volatile VALUE vlist = rb_ary_new2(result.second.size());
    for (const auto& value : result.second) {
      rb_ary_push(vlist, MakeString(value, sfuture->venc));
    }
    volatile VALUE vpair = rb_ary_new2(2);
    rb_ary_push(vpair, MakeStatusValue(std::move(result.first)));
    rb_ary_push(vpair, vlist);
    return vpair;
  }
  if (type == typeid(std::pair<tkrzw::Status, std::map<std::string, std::string>>)) {
    std::pair<tkrzw::Status, std::map<std::string, std::string>> result;
    NativeFunction(sfuture->concurrent, [&]() {
        result = sfuture->future->GetStringMap();
      });
    sfuture->future.reset(nullptr);
    volatile VALUE vhash = rb_hash_new();
    for (const auto& record : result.second) {
      volatile VALUE vkey = MakeString(record.first, sfuture->venc);
      volatile VALUE vvalue = MakeString(record.second, sfuture->venc);
      rb_hash_aset(vhash, vkey, vvalue);
    }
    volatile VALUE vpair = rb_ary_new2(2);
    rb_ary_push(vpair, MakeStatusValue(std::move(result.first)));
    rb_ary_push(vpair, vhash);
    return vpair;
  }
  if (type == typeid(std::pair<tkrzw::Status, int64_t>)) {
    std::pair<tkrzw::Status, int64_t> result;
    NativeFunction(sfuture->concurrent, [&]() {
        result = sfuture->future->GetInteger();
      });
    sfuture->future.reset(nullptr);
    volatile VALUE vpair = rb_ary_new2(2);
    rb_ary_push(vpair, MakeStatusValue(std::move(result.first)));
    rb_ary_push(vpair, LL2NUM(result.second));
    return vpair;
  }
  rb_raise(rb_eRuntimeError, "unimplemented type");
  return Qnil;
}

// Implementation of Future#to_s.
static VALUE future_to_s(VALUE vself) {
  StructFuture* sfuture = nullptr;
  Data_Get_Struct(vself, StructFuture, sfuture);
  if (sfuture->future == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  const std::string str = tkrzw::SPrintF("Future:%p", (void*)sfuture->future.get());
  return rb_str_new(str.data(), str.size());
}

// Implementation of Future#inspect.
static VALUE future_inspect(VALUE vself) {
  StructFuture* sfuture = nullptr;
  Data_Get_Struct(vself, StructFuture, sfuture);
  if (sfuture->future == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  const std::string str = tkrzw::SPrintF("#<Tkrzw::Future: %p>", (void*)sfuture->future.get());
  return rb_str_new(str.data(), str.size());
}

// Creates a future object.
static VALUE MakeFutureValue(tkrzw::StatusFuture&& future, bool concurrent, VALUE venc) {
  StructFuture* sfuture = new StructFuture;
  sfuture->future = std::make_unique<tkrzw::StatusFuture>(std::move(future));
  sfuture->concurrent = false;
  sfuture->venc = Qnil;
  return Data_Wrap_Struct(cls_future, 0, future_del, sfuture);
}

// Defines the Future class.
static void DefineFuture() {
  cls_future = rb_define_class_under(mod_tkrzw, "Future", rb_cObject);
  rb_define_alloc_func(cls_future, future_new);
  rb_define_private_method(cls_future, "initialize", (METHOD)future_initialize, -1);
  rb_define_method(cls_future, "destruct", (METHOD)future_destruct, 0);
  rb_define_method(cls_future, "wait", (METHOD)future_wait, -1);
  rb_define_method(cls_future, "get", (METHOD)future_get, 0);
  rb_define_method(cls_future, "to_s", (METHOD)future_to_s, 0);
  rb_define_method(cls_future, "inspect", (METHOD)future_inspect, 0);
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
  StructDBM* sdbm = (StructDBM*)ptr;
  sdbm->dbm.reset(nullptr);
  delete sdbm;
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
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "sync_hard", "false"))) {
    open_options |= tkrzw::File::OPEN_SYNC_HARD;
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
  params.erase("sync_hard");
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
      sdbm->dbm->GetMulti(key_views, &records);
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
  volatile VALUE voverwrite, vrecords;
  rb_scan_args(argc, argv, "02", &voverwrite, &vrecords);
  bool overwrite = true;
  if (argc <= 1 && TYPE(voverwrite) == T_HASH) {
    vrecords = voverwrite;
  } else {
    overwrite = argc > 0 ? RTEST(voverwrite) : true;
  }
  const auto& records = HashToMap(vrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->SetMulti(record_views, overwrite);
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

// Implementation of DBM#append_multi.
static VALUE dbm_append_multi(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vdelim, vrecords;
  rb_scan_args(argc, argv, "02", &vdelim, &vrecords);
  std::string_view delim = "";
  if (argc <= 1 && TYPE(vdelim) == T_HASH) {
    vrecords = vdelim;
  } else {
    delim = argc > 0 ? GetStringView(vdelim) : std::string_view("");
  }
  const auto& records = HashToMap(vrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->AppendMulti(record_views, delim);
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
    return LL2NUM(current);
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

// Implementation of DBM#rekey.
static VALUE dbm_rekey(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vold_key, vnew_key, voverwrite, vcopying;
  rb_scan_args(argc, argv, "22", &vold_key, &vnew_key, &voverwrite);
  vold_key = StringValueEx(vold_key);
  const std::string_view old_key = GetStringView(vold_key);
  vnew_key = StringValueEx(vnew_key);
  const std::string_view new_key = GetStringView(vnew_key);
  const bool overwrite = argc > 2 ? RTEST(voverwrite) : true;
  const bool copying = argc > 3 ? RTEST(vcopying) : false;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Rekey(old_key, new_key, overwrite, copying);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#pop_first.
static VALUE dbm_pop_first(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vstatus;
  rb_scan_args(argc, argv, "01", &vstatus);
  std::string key, value;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->PopFirst(&key, &value);
    });
  if (rb_obj_is_instance_of(vstatus, cls_status)) {
    SetStatusValue(vstatus, status);
  }
  if (status == tkrzw::Status::SUCCESS) {
    volatile VALUE vary = rb_ary_new2(2);
    rb_ary_push(vary, MakeString(key, sdbm->venc));
    rb_ary_push(vary, MakeString(value, sdbm->venc));
    return vary;
  }
  return Qnil;
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
    return LL2NUM(count);
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
    return LL2NUM(file_size);
  }
  return Qnil;
}

// Implementation of DBM#file_path.
static VALUE dbm_file_path(VALUE vself) {
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

// Implementation of DBM#timestamp.
static VALUE dbm_timestamp(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  double timestamp = 0;

  NativeFunction(sdbm->concurrent, [&]() {
      timestamp = sdbm->dbm->GetTimestampSimple();
    });
  if (timestamp >= 0) {
    return rb_float_new(timestamp);
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
static VALUE dbm_copy_file_data(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vdestpath, vsynchard;
  rb_scan_args(argc, argv, "11", &vdestpath, &vsynchard);
  vdestpath = StringValueEx(vdestpath);
  const std::string_view dest_path = GetStringView(vdestpath);
  const bool sync_hard = RTEST(vsynchard);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->CopyFileData(std::string(dest_path), sync_hard);
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
  StructDBM* sdest_dbm = nullptr;
  Data_Get_Struct(vdestdbm, StructDBM, sdest_dbm);
  if (sdest_dbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = sdbm->dbm->Export(sdest_dbm->dbm.get());
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#export_to_flat_records.
static VALUE dbm_export_to_flat_records(VALUE vself, VALUE vdest_file) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  StructFile* sdest_file = nullptr;
  Data_Get_Struct(vdest_file, StructFile, sdest_file);
  if (sdest_file->file == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened file");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = tkrzw::ExportDBMToFlatRecords(sdbm->dbm.get(), sdest_file->file.get());
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#import_from_flat_records.
static VALUE dbm_import_from_flat_records(VALUE vself, VALUE vsrc_file) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  StructFile* ssrc_file = nullptr;
  Data_Get_Struct(vsrc_file, StructFile, ssrc_file);
  if (ssrc_file->file == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened file");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = tkrzw::ImportDBMFromFlatRecords(sdbm->dbm.get(), ssrc_file->file.get());
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of DBM#export_keys_as_lines.
static VALUE dbm_export_keys_as_lines(VALUE vself, VALUE vdest_file) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  StructFile* sdest_file = nullptr;
  Data_Get_Struct(vdest_file, StructFile, sdest_file);
  if (sdest_file->file == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened file");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
      status = tkrzw::ExportDBMKeysAsLines(sdbm->dbm.get(), sdest_file->file.get());
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

// Implementation of DBM#writable?.
static VALUE dbm_is_writable(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  const bool writable = sdbm->dbm->IsWritable();;
  return writable ? Qtrue : Qfalse;
}

// Implementation of DBM#healthy?.
static VALUE dbm_is_healthy(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  const bool healthy = sdbm->dbm->IsHealthy();
  return healthy ? Qtrue : Qfalse;
}

// Implementation of DBM#ordered?.
static VALUE dbm_is_ordered(VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  const bool ordered = sdbm->dbm->IsOrdered();
  return ordered ? Qtrue : Qfalse;
}

// Implementation of DBM#search.
static VALUE dbm_search(int argc, VALUE* argv, VALUE vself) {
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vself, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  volatile VALUE vmode, vpattern, vcapacity;
  rb_scan_args(argc, argv, "21", &vmode, &vpattern, &vcapacity);
  vmode = StringValueEx(vmode);
  const std::string_view mode = GetStringView(vmode);
  vpattern = StringValueEx(vpattern);
  const std::string_view pattern = GetStringView(vpattern);
  const int64_t capacity = GetInteger(vcapacity);
  std::vector<std::string> keys;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sdbm->concurrent, [&]() {
    status = tkrzw::SearchDBMModal(sdbm->dbm.get(), mode, pattern, &keys, capacity);
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
      "#<Tkrzw::DBM:", class_name, ":", tkrzw::StrEscapeC(path, true), ":", count, ">");
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

// Implementation of DBM#delete.
static VALUE dbm_delete(VALUE vself, VALUE vkey) {
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
  if (status != tkrzw::Status::SUCCESS) {
    return Qnil;
  }
  return MakeString(old_value, sdbm->venc);
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
  rb_define_method(cls_dbm, "append_multi", (METHOD)dbm_append_multi, -1);
  rb_define_method(cls_dbm, "compare_exchange", (METHOD)dbm_compare_exchange, 3);
  rb_define_method(cls_dbm, "increment", (METHOD)dbm_increment, -1);
  rb_define_method(cls_dbm, "compare_exchange_multi", (METHOD)dbm_compare_exchange_multi, 2);
  rb_define_method(cls_dbm, "rekey", (METHOD)dbm_rekey, -1);
  rb_define_method(cls_dbm, "pop_first", (METHOD)dbm_pop_first, -1);
  rb_define_method(cls_dbm, "count", (METHOD)dbm_count, 0);
  rb_define_method(cls_dbm, "file_size", (METHOD)dbm_file_size, 0);
  rb_define_method(cls_dbm, "file_path", (METHOD)dbm_file_path, 0);
  rb_define_method(cls_dbm, "timestamp", (METHOD)dbm_timestamp, 0);
  rb_define_method(cls_dbm, "clear", (METHOD)dbm_clear, 0);
  rb_define_method(cls_dbm, "rebuild", (METHOD)dbm_rebuild, -1);
  rb_define_method(cls_dbm, "should_be_rebuilt?", (METHOD)dbm_should_be_rebuilt, 0);
  rb_define_method(cls_dbm, "synchronize", (METHOD)dbm_synchronize, -1);
  rb_define_method(cls_dbm, "copy_file_data", (METHOD)dbm_copy_file_data, -1);
  rb_define_method(cls_dbm, "export", (METHOD)dbm_export, 1);
  rb_define_method(cls_dbm, "export_to_flat_records", (METHOD)dbm_export_to_flat_records, 1);
  rb_define_method(cls_dbm, "import_from_flat_records", (METHOD)dbm_import_from_flat_records, 1);
  rb_define_method(cls_dbm, "export_keys_as_lines", (METHOD)dbm_export_keys_as_lines, 1);
  rb_define_method(cls_dbm, "inspect_details", (METHOD)dbm_inspect_details, 0);
  rb_define_method(cls_dbm, "open?", (METHOD)dbm_is_open, 0);
  rb_define_method(cls_dbm, "writable?", (METHOD)dbm_is_writable, 0);
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
  rb_define_method(cls_dbm, "delete", (METHOD)dbm_delete, 1);
  rb_define_method(cls_dbm, "each", (METHOD)dbm_each, 0);
}

// Implementation of Iterator#del.
static void iter_del(void* ptr) {
  StructIter* siter = (StructIter*)ptr;
  siter->iter.reset(nullptr);
  delete siter;
}

// Implementation of Iterator.new.
static VALUE iter_new(VALUE cls) {
  StructIter* siter = new StructIter;
  return Data_Wrap_Struct(cls_iter, 0, iter_del, siter);
}

// Implementation of Iterator#initialize.
static VALUE iter_initialize(VALUE vself, VALUE vdbm) {
  if (!rb_obj_is_instance_of(vdbm, cls_dbm)) {
    rb_raise(rb_eArgError, "#<Tkrzw::StatusException>");
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

// Implementation of Iterator#step.
static VALUE iter_step(int argc, VALUE* argv, VALUE vself) {
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
      status = siter->iter->Step(&key, &value);
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
      tkrzw::StrCat("#<Tkrzw::Iterator:", tkrzw::StrEscapeC(key, true), ">");
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
  rb_define_method(cls_iter, "step", (METHOD)iter_step, -1);
  rb_define_method(cls_iter, "to_s", (METHOD)iter_to_s, 0);
  rb_define_method(cls_iter, "inspect", (METHOD)iter_inspect, 0);
}

// Implementation of AsyncDBM#del.
static void asyncdbm_del(void* ptr) {
  StructAsyncDBM* sasync = (StructAsyncDBM*)ptr;
  sasync->async.reset(nullptr);
  delete sasync;
}

// Implementation of AsyncDBM.new.
static VALUE asyncdbm_new(VALUE cls) {
  StructAsyncDBM* sasync = new StructAsyncDBM;
  return Data_Wrap_Struct(cls_asyncdbm, 0, asyncdbm_del, sasync);
}

// Implementation of AsyncDBM#initialize.
static VALUE asyncdbm_initialize(int argc, VALUE* argv, VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  volatile VALUE vdbm, vnum_threads;
  rb_scan_args(argc, argv, "20", &vdbm, &vnum_threads);
  StructDBM* sdbm = nullptr;
  Data_Get_Struct(vdbm, StructDBM, sdbm);
  if (sdbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  const int32_t num_threads = GetInteger(vnum_threads);
  sasync->async = std::make_unique<tkrzw::AsyncDBM>(sdbm->dbm.get(), num_threads);
  return Qnil;
}

// Implementation of AsyncDBM#destruct.
static VALUE asyncdbm_destruct(VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  sasync->async.reset(nullptr);
  return Qnil;
}

// Implementation of AsyncDBM#to_s.
static VALUE asyncdbm_to_s(VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  const std::string str = tkrzw::SPrintF("AsyncDBM:%p", (void*)sasync->async.get());
  return rb_str_new(str.data(), str.size());
}

// Implementation of AsyncDBM#inspect.
static VALUE asyncdbm_inspect(VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  const std::string str = tkrzw::SPrintF(
      "#<Tkrzw::AsyncDBM: %p>", (void*)sasync->async.get());
  return rb_str_new(str.data(), str.size());
}

// Implementation of AsyncDBM#get.
static VALUE asyncdbm_get(VALUE vself, VALUE vkey) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  tkrzw::StatusFuture future(sasync->async->Get(key));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#get_multi.
static VALUE asyncdbm_get_multi(VALUE vself, VALUE vkeys) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
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
  tkrzw::StatusFuture future(sasync->async->GetMulti(key_views));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#set.
static VALUE asyncdbm_set(int argc, VALUE* argv, VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  volatile VALUE vkey, vvalue, voverwrite;
  rb_scan_args(argc, argv, "21", &vkey, &vvalue, &voverwrite);
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  vvalue = StringValueEx(vvalue);
  const std::string_view value = GetStringView(vvalue);
  const bool overwrite = argc > 2 ? RTEST(voverwrite) : true;
  tkrzw::StatusFuture future(sasync->async->Set(key, value, overwrite));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#set_multi.
static VALUE asyncdbm_set_multi(int argc, VALUE* argv, VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  volatile VALUE voverwrite, vrecords;
  rb_scan_args(argc, argv, "02", &voverwrite, &vrecords);
  bool overwrite = true;
  if (argc <= 1 && TYPE(voverwrite) == T_HASH) {
    vrecords = voverwrite;
  } else {
    overwrite = argc > 0 ? RTEST(voverwrite) : true;
  }
  const auto& records = HashToMap(vrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  tkrzw::StatusFuture future(sasync->async->SetMulti(record_views, overwrite));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#remove.
static VALUE asyncdbm_remove(VALUE vself, VALUE vkey) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  tkrzw::StatusFuture future(sasync->async->Remove(key));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#remove_multi.
static VALUE asyncdbm_remove_multi(VALUE vself, VALUE vkeys) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
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
  tkrzw::StatusFuture future(sasync->async->RemoveMulti(key_views));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#append.
static VALUE asyncdbm_append(int argc, VALUE* argv, VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  volatile VALUE vkey, vvalue, vdelim;
  rb_scan_args(argc, argv, "21", &vkey, &vvalue, &vdelim);
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  vvalue = StringValueEx(vvalue);
  const std::string_view value = GetStringView(vvalue);
  vdelim = StringValueEx(vdelim);
  const std::string_view delim = GetStringView(vdelim);
  tkrzw::StatusFuture future(sasync->async->Append(key, value, delim));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#append_multi.
static VALUE asyncdbm_append_multi(int argc, VALUE* argv, VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  volatile VALUE vdelim, vrecords;
  rb_scan_args(argc, argv, "02", &vdelim, &vrecords);
  std::string_view delim = "";
  if (argc <= 1 && TYPE(vdelim) == T_HASH) {
    vrecords = vdelim;
  } else {
    delim = argc > 0 ? GetStringView(vdelim) : std::string_view("");
  }
  const auto& records = HashToMap(vrecords);
  std::map<std::string_view, std::string_view> record_views;
  for (const auto& record : records) {
    record_views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  tkrzw::StatusFuture future(sasync->async->AppendMulti(record_views, delim));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#compare_exchange.
static VALUE asyncdbm_compare_exchange(VALUE vself, VALUE vkey, VALUE vexpected, VALUE vdesired) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
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
  tkrzw::StatusFuture future(sasync->async->CompareExchange(key, expected, desired));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#increment.
static VALUE asyncdbm_increment(int argc, VALUE* argv, VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  volatile VALUE vkey, vinc, vinit, vstatus;
  rb_scan_args(argc, argv, "13", &vkey, &vinc, &vinit, &vstatus);
  vkey = StringValueEx(vkey);
  const std::string_view key = GetStringView(vkey);
  const int64_t inc = vinc == Qnil ? 1 : GetInteger(vinc);
  const int64_t init = vinit == Qnil ? 0 : GetInteger(vinit);
  tkrzw::StatusFuture future(sasync->async->Increment(key, inc, init));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#compare_exchange_multi.
static VALUE asyncdbm_compare_exchange_multi(VALUE vself, VALUE vexpected, VALUE vdesired) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  if (TYPE(vexpected) != T_ARRAY || TYPE(vdesired) != T_ARRAY) {
    rb_raise(rb_eRuntimeError, "expected or desired is not an array");
  }
  const auto& expected = ExtractSVPairs(vexpected);
  const auto& desired = ExtractSVPairs(vdesired);
  tkrzw::StatusFuture future(sasync->async->CompareExchangeMulti(expected, desired));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#clear.
static VALUE asyncdbm_clear(VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  tkrzw::StatusFuture future(sasync->async->Clear());
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#rebuild.
static VALUE asyncdbm_rebuild(int argc, VALUE* argv, VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  volatile VALUE vparams;
  rb_scan_args(argc, argv, "01", &vparams);
  const std::map<std::string, std::string> params = HashToMap(vparams);
  tkrzw::StatusFuture future(sasync->async->Rebuild(params));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#synchronize.
static VALUE asyncdbm_synchronize(int argc, VALUE* argv, VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  volatile VALUE vhard, vparams;
  rb_scan_args(argc, argv, "11", &vhard, &vparams);
  const bool hard = RTEST(vhard);
  const std::map<std::string, std::string> params = HashToMap(vparams);
  tkrzw::StatusFuture future(sasync->async->Synchronize(hard, nullptr, params));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#copy_file_data.
static VALUE asyncdbm_copy_file_data(int argc, VALUE* argv, VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  volatile VALUE vdestpath, vsynchard;
  rb_scan_args(argc, argv, "11", &vdestpath, &vsynchard);
  vdestpath = StringValueEx(vdestpath);
  const std::string_view dest_path = GetStringView(vdestpath);
  const bool sync_hard = RTEST(vsynchard);
  tkrzw::StatusFuture future(sasync->async->CopyFileData(std::string(dest_path), sync_hard));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#export.
static VALUE asyncdbm_export(VALUE vself, VALUE vdestdbm) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  if (!rb_obj_is_instance_of(vdestdbm, cls_dbm)) {
    return MakeStatusValue(tkrzw::Status(tkrzw::Status::INVALID_ARGUMENT_ERROR));
  }
  StructDBM* sdest_dbm = nullptr;
  Data_Get_Struct(vdestdbm, StructDBM, sdest_dbm);
  if (sdest_dbm->dbm == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened database");
  }
  tkrzw::StatusFuture future(sasync->async->Export(sdest_dbm->dbm.get()));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#export_to_flat_records.
static VALUE asyncdbm_export_to_flat_records(VALUE vself, VALUE vdest_file) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  StructFile* sdest_file = nullptr;
  Data_Get_Struct(vdest_file, StructFile, sdest_file);
  if (sdest_file->file == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened file");
  }
  tkrzw::StatusFuture future(sasync->async->ExportToFlatRecords(sdest_file->file.get()));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#import_from_flat_records.
static VALUE asyncdbm_import_from_flat_records(VALUE vself, VALUE vsrc_file) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  StructFile* ssrc_file = nullptr;
  Data_Get_Struct(vsrc_file, StructFile, ssrc_file);
  if (ssrc_file->file == nullptr) {
    rb_raise(rb_eRuntimeError, "not opened file");
  }
  tkrzw::StatusFuture future(sasync->async->ImportFromFlatRecords(ssrc_file->file.get()));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Implementation of AsyncDBM#search.
static VALUE asyncdbm_search(int argc, VALUE* argv, VALUE vself) {
  StructAsyncDBM* sasync = nullptr;
  Data_Get_Struct(vself, StructAsyncDBM, sasync);
  if (sasync->async == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed object");
  }
  volatile VALUE vmode, vpattern, vcapacity;
  rb_scan_args(argc, argv, "21", &vmode, &vpattern, &vcapacity);
  vmode = StringValueEx(vmode);
  const std::string_view mode = GetStringView(vmode);
  vpattern = StringValueEx(vpattern);
  const std::string_view pattern = GetStringView(vpattern);
  const int64_t capacity = GetInteger(vcapacity);
  tkrzw::StatusFuture future(sasync->async->SearchModal(mode, pattern, capacity));
  return MakeFutureValue(std::move(future), sasync->concurrent, sasync->venc);
}

// Defines the AsyncDBM class.
static void DefineAsyncDBM() {
  cls_asyncdbm = rb_define_class_under(mod_tkrzw, "AsyncDBM", rb_cObject);
  rb_define_alloc_func(cls_asyncdbm, asyncdbm_new);
  rb_define_private_method(cls_asyncdbm, "initialize", (METHOD)asyncdbm_initialize, -1);
  rb_define_method(cls_asyncdbm, "destruct", (METHOD)asyncdbm_destruct, 0);
  rb_define_method(cls_asyncdbm, "get", (METHOD)asyncdbm_get, 1);
  rb_define_method(cls_asyncdbm, "get_multi", (METHOD)asyncdbm_get_multi, -2);
  rb_define_method(cls_asyncdbm, "set", (METHOD)asyncdbm_set, -1);
  rb_define_method(cls_asyncdbm, "set_multi", (METHOD)asyncdbm_set_multi, -1);
  rb_define_method(cls_asyncdbm, "remove", (METHOD)asyncdbm_remove, 1);
  rb_define_method(cls_asyncdbm, "remove_multi", (METHOD)asyncdbm_remove_multi, -2);
  rb_define_method(cls_asyncdbm, "append", (METHOD)asyncdbm_append, -1);
  rb_define_method(cls_asyncdbm, "append_multi", (METHOD)asyncdbm_append_multi, -1);
  rb_define_method(cls_asyncdbm, "compare_exchange", (METHOD)asyncdbm_compare_exchange, 3);
  rb_define_method(cls_asyncdbm, "increment", (METHOD)asyncdbm_increment, -1);
  rb_define_method(cls_asyncdbm, "compare_exchange_multi",
                   (METHOD)asyncdbm_compare_exchange_multi, 2);
  rb_define_method(cls_asyncdbm, "clear", (METHOD)asyncdbm_clear, 0);
  rb_define_method(cls_asyncdbm, "rebuild", (METHOD)asyncdbm_rebuild, -1);
  rb_define_method(cls_asyncdbm, "synchronize", (METHOD)asyncdbm_synchronize, -1);
  rb_define_method(cls_asyncdbm, "copy_file_data", (METHOD)asyncdbm_copy_file_data, -1);
  rb_define_method(cls_asyncdbm, "export", (METHOD)asyncdbm_export, 1);
  rb_define_method(cls_asyncdbm, "export_to_flat_records",
                   (METHOD)asyncdbm_export_to_flat_records, 1);
  rb_define_method(cls_asyncdbm, "import_from_flat_records",
                   (METHOD)asyncdbm_import_from_flat_records, 1);
  rb_define_method(cls_asyncdbm, "search", (METHOD)asyncdbm_search, -1);
  rb_define_method(cls_asyncdbm, "to_s", (METHOD)asyncdbm_to_s, 0);
  rb_define_method(cls_asyncdbm, "inspect", (METHOD)asyncdbm_inspect, 0);
}

// Implementation of File#del.
static void file_del(void* ptr) {
  StructFile* sfile = (StructFile*)ptr;
  sfile->file.reset(nullptr);
  delete sfile;
}

// Implementation of File.new.
static VALUE file_new(VALUE cls) {
  StructFile* sfile = new StructFile(new tkrzw::PolyFile);
  return Data_Wrap_Struct(cls_file, 0, file_del, sfile);
}

// Implementation of File#initialize.
static VALUE file_initialize(VALUE vself) {
  return Qnil;
}

// Implementation of File#destruct.
static VALUE file_destruct(VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  sfile->file.reset(nullptr);
  return Qnil;
}

// Implementation of File#open.
static VALUE file_open(int argc, VALUE* argv, VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed File");
  }
  volatile VALUE vpath, vwritable, vparams;
  rb_scan_args(argc, argv, "21", &vpath, &vwritable, &vparams);
  vpath = StringValueEx(vpath);
  const std::string_view path = GetStringView(vpath);
  const bool writable = RTEST(vwritable);  
  std::map<std::string, std::string> params = HashToMap(vparams);
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
  if (tkrzw::StrToBool(tkrzw::SearchMap(params, "sync_hard", "false"))) {
    open_options |= tkrzw::File::OPEN_SYNC_HARD;
  }
  std::string encoding = tkrzw::SearchMap(params, "encoding", "");
  if (encoding.empty()) {
    encoding = "ASCII-8BIT";
  }
  sfile->concurrent = concurrent;
  sfile->venc = GetEncoding(encoding);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sfile->concurrent, [&]() {
      status = sfile->file->OpenAdvanced(std::string(path), writable, open_options, params);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of File#close.
static VALUE file_close(VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed File");
  }
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(true, [&]() {
      status = sfile->file->Close();
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of File#read.
static VALUE file_read(int argc, VALUE* argv, VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed File");
  }
  volatile VALUE voff, vsize, vstatus;
  rb_scan_args(argc, argv, "21", &voff, &vsize, &vstatus);
  const int64_t off = std::max<int64_t>(0, GetInteger(voff));
  const int64_t size =  std::max<int64_t>(0, GetInteger(vsize));
  char* buf = new char[size];
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sfile->concurrent, [&]() {
      status = sfile->file->Read(off, buf, size);
    });
  if (rb_obj_is_instance_of(vstatus, cls_status)) {
    SetStatusValue(vstatus, status);
  }
  if (status == tkrzw::Status::SUCCESS) {
    volatile VALUE vdata = MakeString(std::string_view(buf, size), sfile->venc);
    delete[] buf;
    return vdata;
  }
  delete[] buf;
  return Qnil;
}

// Implementation of File#write.
static VALUE file_write(VALUE vself, VALUE voff, VALUE vdata) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed File");
  }
  const int64_t off = std::max<int64_t>(0, GetInteger(voff));
  vdata = StringValueEx(vdata);
  const std::string_view data = GetStringView(vdata);
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sfile->concurrent, [&]() {
      status = sfile->file->Write(off, data.data(), data.size());
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of File#append.
static VALUE file_append(int argc, VALUE* argv, VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed File");
  }
  volatile VALUE vdata, vstatus;
  rb_scan_args(argc, argv, "11", &vdata, &vstatus);
  vdata = StringValueEx(vdata);
  const std::string_view data = GetStringView(vdata);
  int64_t new_off = 0;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sfile->concurrent, [&]() {
      status = sfile->file->Append(data.data(), data.size(), &new_off);
    });
  if (rb_obj_is_instance_of(vstatus, cls_status)) {
    SetStatusValue(vstatus, status);
  }
  if (status == tkrzw::Status::SUCCESS) {
    return LL2NUM(new_off);
  }
  return Qnil;
}

// Implementation of File#truncate.
static VALUE file_truncate(VALUE vself, VALUE vsize) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed File");
  }
  const int64_t size = std::max<int64_t>(0, GetInteger(vsize));
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(true, [&]() {
      status = sfile->file->Truncate(size);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of File#synchronize.
static VALUE file_synchronize(int argc, VALUE* argv, VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed File");
  }
  volatile VALUE vhard, voff, vsize;
  rb_scan_args(argc, argv, "12", &vhard, &voff, &vsize);
  const bool hard = RTEST(vhard);
  const int64_t off = voff == Qnil ? 0 : std::max<int64_t>(0, GetInteger(voff));
  const int64_t size = vsize == Qnil ? 0 : std::max<int64_t>(0, GetInteger(vsize));
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sfile->concurrent, [&]() {
      status = sfile->file->Synchronize(hard, off, size);
    });
  return MakeStatusValue(std::move(status));
}

// Implementation of File#get_size.
static VALUE file_get_size(VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed File");
  }
  int64_t size = 0;
  NativeFunction(sfile->concurrent, [&]() {
      size = sfile->file->GetSizeSimple();
    });
  if (size >= 0) {
    return LL2NUM(size);
  }
  return Qnil;
}

// Implementation of File#get_path.
static VALUE file_get_path(VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed File");
  }
  std::string path;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sfile->concurrent, [&]() {
      status = sfile->file->GetPath(&path);
    });
  if (status == tkrzw::Status::SUCCESS) {
    return rb_str_new(path.data(), path.size());
  }
  return Qnil;
}

// Implementation of File#search.
static VALUE file_search(int argc, VALUE* argv, VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  if (sfile->file == nullptr) {
    rb_raise(rb_eRuntimeError, "destructed File");
  }
  volatile VALUE vmode, vpattern, vcapacity;
  rb_scan_args(argc, argv, "21", &vmode, &vpattern, &vcapacity);
  vmode = StringValueEx(vmode);
  const std::string_view mode = GetStringView(vmode);
  vpattern = StringValueEx(vpattern);
  const std::string_view pattern = GetStringView(vpattern);
  const int64_t capacity = GetInteger(vcapacity);
  std::vector<std::string> lines;
  tkrzw::Status status(tkrzw::Status::SUCCESS);
  NativeFunction(sfile->concurrent, [&]() {
    status = tkrzw::SearchTextFileModal(sfile->file.get(), mode, pattern, &lines, capacity);
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

// Implementation of File#to_s.
static VALUE file_to_s(VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  std::string class_name = "unknown";
  auto* in_file = sfile->file->GetInternalFile();
  if (in_file != nullptr) {
    const auto& file_type = in_file->GetType();
    if (file_type == typeid(tkrzw::StdFile)) {
      class_name = "StdFile";
    } else if (file_type == typeid(tkrzw::MemoryMapParallelFile)) {
      class_name = "MemoryMapParallelFile";
    } else if (file_type == typeid(tkrzw::MemoryMapAtomicFile)) {
      class_name = "MemoryMapAtomicFile";
    } else if (file_type == typeid(tkrzw::PositionalParallelFile)) {
      class_name = "PositionalParallelFile";
    } else if (file_type == typeid(tkrzw::PositionalAtomicFile)) {
      class_name = "PositionalAtomicFile";
    }
  }
  const std::string path = sfile->file->GetPathSimple();
  const int64_t size = sfile->file->GetSizeSimple();
  const std::string expr =
      tkrzw::StrCat(class_name, ":", tkrzw::StrEscapeC(path, true), ":", size);
  return rb_str_new(expr.data(), expr.size());
}

// Implementation of File#inspect.
static VALUE file_inspect(VALUE vself) {
  StructFile* sfile = nullptr;
  Data_Get_Struct(vself, StructFile, sfile);
  std::string class_name = "unknown";
  auto* in_file = sfile->file->GetInternalFile();
  if (in_file != nullptr) {
    const auto& file_type = in_file->GetType();
    if (file_type == typeid(tkrzw::StdFile)) {
      class_name = "StdFile";
    } else if (file_type == typeid(tkrzw::MemoryMapParallelFile)) {
      class_name = "MemoryMapParallelFile";
    } else if (file_type == typeid(tkrzw::MemoryMapAtomicFile)) {
      class_name = "MemoryMapAtomicFile";
    } else if (file_type == typeid(tkrzw::PositionalParallelFile)) {
      class_name = "PositionalParallelFile";
    } else if (file_type == typeid(tkrzw::PositionalAtomicFile)) {
      class_name = "PositionalAtomicFile";
    }
  }
  const std::string path = sfile->file->GetPathSimple();
  const int64_t size = sfile->file->GetSizeSimple();
  const std::string expr = tkrzw::StrCat(
      "#<Tkrzw::File:", class_name, ":", tkrzw::StrEscapeC(path, true), ":", size, ">");
  return rb_str_new(expr.data(), expr.size());
}

// Defines the File class.
static void DefineFile() {
  cls_file = rb_define_class_under(mod_tkrzw, "File", rb_cObject);
  rb_define_alloc_func(cls_file, file_new);
  rb_define_private_method(cls_file, "initialize", (METHOD)file_initialize, 0);
  rb_define_method(cls_file, "destruct", (METHOD)file_destruct, 0);
  rb_define_method(cls_file, "open", (METHOD)file_open, -1);
  rb_define_method(cls_file, "close", (METHOD)file_close, 0);
  rb_define_method(cls_file, "read", (METHOD)file_read, -1);
  rb_define_method(cls_file, "write", (METHOD)file_write, 2);
  rb_define_method(cls_file, "append", (METHOD)file_append, -1);
  rb_define_method(cls_file, "truncate", (METHOD)file_truncate, 1);
  rb_define_method(cls_file, "synchronize", (METHOD)file_synchronize, -1);
  rb_define_method(cls_file, "get_size", (METHOD)file_get_size, 0);
  rb_define_method(cls_file, "get_path", (METHOD)file_get_path, 0);
  rb_define_method(cls_file, "search", (METHOD)file_search, -1);
  rb_define_method(cls_file, "to_s", (METHOD)file_to_s, 0);
  rb_define_method(cls_file, "inspect", (METHOD)file_inspect, 0);
}

// Entry point of the library.
void Init_tkrzw() {
  DefineModule();
  DefineUtility();
  DefineStatus();
  DefineFuture();
  DefineStatusException();
  DefineDBM();
  DefineIterator();
  DefineAsyncDBM();
  DefineFile();
}

}  // extern "C"

// END OF FILE
