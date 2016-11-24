#include <leveldb/c.h>
#include <string.h>
#include <nan.h>
#include <map>

using v8::Exception;
using v8::FunctionCallbackInfo;
using v8::Isolate;
using v8::Local;
using v8::Number;
using v8::Object;
using v8::String;
using v8::Value;

namespace addon {
  leveldb_t *db;

  static uint32_t iteratorCount = 0;
  static std::map<uint32_t, leveldb_iterator_t*> iterators;
  static std::map<uint32_t, leveldb_readoptions_t*> iteratorOptions;

  const char* ToCString(const String::Utf8Value& value) {
    return *value ? *value : "string conversion failed";
  }

  void Open(const Nan::FunctionCallbackInfo<v8::Value>& info) {

    if (info.Length() != 1) {
      Nan::ThrowTypeError("Wrong number of arguments");
      return;
    }

    if (!info[0]->IsString()) {
      Nan::ThrowTypeError("Wrong type of arguments");
      return;
    }

    String::Utf8Value str(info[0]);
    const char* arg = ToCString(str);

    if(strlen(arg) == 0) {
      Nan::ThrowError("Database name is empty");
      return;
    } else {
      leveldb_options_t *options;
      options = leveldb_options_create();
      leveldb_options_set_create_if_missing(options, 1);

      leveldb_options_set_compression(options, 2);

      char *err = NULL;
      db = leveldb_open(options, arg, &err);

      if (err != NULL) {
        Nan::ThrowError("Failed to open database");
        return;
      }
      leveldb_free(err); err = NULL;

      leveldb_options_destroy(options);
      info.GetReturnValue().Set(Nan::New("opened").ToLocalChecked());
    }
  }

  void Close(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    leveldb_close(db);
    info.GetReturnValue().Set(Nan::New("closed").ToLocalChecked());
  }

  void Get(const Nan::FunctionCallbackInfo<v8::Value>& info) {

    if (info.Length() != 1) {
     Nan::ThrowTypeError("Wrong number of arguments");
     return;
    }

    if (!info[0]->IsString()) {
     Nan::ThrowTypeError("Wrong arguments");
     return;
    }

    String::Utf8Value str(info[0]);
    const char* arg = ToCString(str);

    leveldb_readoptions_t *roptions;
    roptions = leveldb_readoptions_create();

    char *read;
    size_t read_len;
    char *err = NULL;
    read = leveldb_get(db, roptions, arg, str.length(), &read_len, &err);

    if (err != NULL) {
      Nan::ThrowError("Read fail");
      return;
    }
    leveldb_free(err); err = NULL;

    leveldb_readoptions_destroy(roptions);
    info.GetReturnValue().Set(Nan::New(read,read_len).ToLocalChecked());
  }

  void Put(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    if (info.Length() != 2) {
      Nan::ThrowTypeError("Wrong number of arguments");
      return;
    }

    if (!info[0]->IsString() && !info[1]->IsString()) {
      Nan::ThrowTypeError("Wrong arguments");
      return;
    }

    String::Utf8Value str(info[0]);
    const char* arg = ToCString(str);

    String::Utf8Value str2(info[1]);
    const char* arg2 = ToCString(str2);

    leveldb_writeoptions_t *woptions;
    woptions = leveldb_writeoptions_create();
    char *err = NULL;
    leveldb_put(db, woptions, arg, str.length(), arg2, str2.length(), &err);

    if (err != NULL) {
      Nan::ThrowError("Write fail");
      return;
    }
    leveldb_free(err); err = NULL;

    leveldb_writeoptions_destroy(woptions);
    info.GetReturnValue().Set(Nan::New("written").ToLocalChecked());
  }

  void Delete(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    if (info.Length() != 1) {
      Nan::ThrowTypeError("Wrong number of arguments");
      return;
    }

    if (!info[0]->IsString()) {
      Nan::ThrowTypeError("Wrong arguments");
      return;
    }

    String::Utf8Value str(info[0]);
    const char* arg = ToCString(str);

    leveldb_writeoptions_t *woptions;
    woptions = leveldb_writeoptions_create();
    char *err = NULL;
    leveldb_delete(db, woptions, arg, str.length(), &err);

    if (err != NULL) {
      Nan::ThrowError("Delete fail");
      return;
    }
    leveldb_free(err); err = NULL;

    info.GetReturnValue().Set(Nan::New("deleted").ToLocalChecked());
  }


  void IterNew(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    if (info.Length() != 0) {
      Nan::ThrowTypeError("Wrong number of arguments");
      return;
    }

    leveldb_readoptions_t *roptions;
    roptions = leveldb_readoptions_create();

    leveldb_iterator_t* iterator = leveldb_create_iterator(db, roptions);
    leveldb_iter_seek_to_first(iterator);
    uint32_t id = iteratorCount++;
    iterators[id] = iterator;
    iteratorOptions[id] = roptions;

    info.GetReturnValue().Set(Nan::New((uint32_t)id));
  }

  void IterNext(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    if (info.Length() != 1) {
      Nan::ThrowTypeError("Wrong number of arguments");
      return;
    }

    uint32_t id = (uint32_t)v8::Number::Cast(*info[0])->Value();
    leveldb_iterator_t* iter = iterators[id];
    leveldb_iter_next(iter);

    info.GetReturnValue().Set(Nan::New((uint32_t)id));
  }

  void IterValid(const Nan::FunctionCallbackInfo<v8::Value>& info) {
    if (info.Length() != 1) {
      Nan::ThrowTypeError("Wrong number of arguments");
      return;
    }

    uint32_t id = (uint32_t)v8::Number::Cast(*info[0])->Value();
    uint8_t valid = leveldb_iter_valid(iterators[id]);

    info.GetReturnValue().Set(Nan::New(valid == 0 ? 0 : (id == 0 ? 1 : id)));
  }

    void IterDestroy(const Nan::FunctionCallbackInfo<v8::Value>& info) {
        if (info.Length() != 1) {
            Nan::ThrowTypeError("Wrong number of arguments");
            return;
        }

        uint32_t id = (uint32_t)v8::Number::Cast(*info[0])->Value();
        leveldb_iter_destroy(iterators[id]);
        iterators.erase(id);
    }

    void IterKey(const Nan::FunctionCallbackInfo<v8::Value>& info) {
        if (info.Length() != 1) {
            Nan::ThrowTypeError("Wrong number of arguments");
            return;
        }

        uint32_t id = (uint32_t)v8::Number::Cast(*info[0])->Value();
        size_t keylen;
        const char* key = leveldb_iter_key(iterators[id], &keylen);

        info.GetReturnValue().Set(Nan::New(key, keylen).ToLocalChecked());
    }

    void IterValue(const Nan::FunctionCallbackInfo<v8::Value>& info) {
        if (info.Length() != 1) {
            Nan::ThrowTypeError("Wrong number of arguments");
            return;
        }

        uint32_t id = (uint32_t)v8::Number::Cast(*info[0])->Value();
        size_t valuelen;
        const char* value= leveldb_iter_value(iterators[id], &valuelen);

        info.GetReturnValue().Set(Nan::New(value, valuelen).ToLocalChecked());
    }

  void Init(v8::Local<v8::Object> exports) {
    exports->Set(Nan::New("open").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(Open)->GetFunction());
    exports->Set(Nan::New("close").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(Close)->GetFunction());
    exports->Set(Nan::New("get").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(Get)->GetFunction());
    exports->Set(Nan::New("put").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(Put)->GetFunction());
    exports->Set(Nan::New("delete").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(Delete)->GetFunction());
    exports->Set(Nan::New("iter_new").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(IterNew)->GetFunction());
    exports->Set(Nan::New("iter_next").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(IterNext)->GetFunction());
    exports->Set(Nan::New("iter_valid").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(IterValid)->GetFunction());
    exports->Set(Nan::New("iter_destroy").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(IterDestroy)->GetFunction());
    exports->Set(Nan::New("iter_key").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(IterKey)->GetFunction());
    exports->Set(Nan::New("iter_value").ToLocalChecked(),
      Nan::New<v8::FunctionTemplate>(IterValue)->GetFunction());
  }

  NODE_MODULE(addon, Init)
}
