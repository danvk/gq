// JSON filterkey example which populates filtered SAX events into a Document.

// This example parses JSON text from stdin with validation.
// During parsing, specified key will be filtered using a SAX handler.
// And finally the filtered events are used to populate a Document.
// As an example, the document is written to standard output.

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/error/en.h"
#include <map>
#include <string>
#include <stack>
#include <iostream>

using namespace rapidjson;
using namespace std;

#define ASSERT(condition, message) \
    do { \
        if (! (condition)) { \
            std::cerr << "Assertion `" #condition "` failed in " << __FILE__ \
                      << " line " << __LINE__ << ": " << message << std::endl; \
            std::exit(EXIT_FAILURE); \
        } \
    } while (false)

// This handler forwards event into an output handler, with filtering the descendent events of specified key.
template <typename OutputHandler>
class FilterKeyHandler {
public:
  typedef char Ch;

  FilterKeyHandler(OutputHandler& outputHandler, const Ch* keyString, SizeType keyLength) : 
    outputHandler_(outputHandler), keyString_(keyString), keyLength_(keyLength), filterValueDepth_(), filteredKeyCount_()
  {}

  bool Null()       { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Null()  && EndValue(); }
  bool Bool(bool b)     { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Bool(b)   && EndValue(); }
  bool Int(int i)     { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Int(i)  && EndValue(); }
  bool Uint(unsigned u)   { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Uint(u)   && EndValue(); }
  bool Int64(int64_t i)   { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Int64(i)  && EndValue(); }
  bool Uint64(uint64_t u) { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Uint64(u) && EndValue(); }
  bool Double(double d)   { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.Double(d) && EndValue(); }
  bool RawNumber(const Ch* str, SizeType len, bool copy) { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.RawNumber(str, len, copy) && EndValue(); }
  bool String   (const Ch* str, SizeType len, bool copy) { return filterValueDepth_ > 0 ? EndValue() : outputHandler_.String   (str, len, copy) && EndValue(); }
  
  bool StartObject() { 
    if (filterValueDepth_ > 0) {
      filterValueDepth_++;
      return true;
    }
    else {
      filteredKeyCount_.push(0);
      return outputHandler_.StartObject();
    }
  }
  
  bool Key(const Ch* str, SizeType len, bool copy) { 
    if (filterValueDepth_ > 0) 
      return true;
    else if (len == keyLength_ && std::memcmp(str, keyString_, len) == 0) {
      filterValueDepth_ = 1;
      return true;
    }
    else {
      ++filteredKeyCount_.top();
      return outputHandler_.Key(str, len, copy);
    }
  }

  bool EndObject(SizeType) {
    if (filterValueDepth_ > 0) {
      filterValueDepth_--;
      return EndValue();
    }
    else {
      // Use our own filtered memberCount
      SizeType memberCount = filteredKeyCount_.top();
      filteredKeyCount_.pop();
      return outputHandler_.EndObject(memberCount) && EndValue();
    }
  }

  bool StartArray() {
    if (filterValueDepth_ > 0) {
      filterValueDepth_++;
      return true;
    }
    else
      return outputHandler_.StartArray();
  }

  bool EndArray(SizeType elementCount) {
    if (filterValueDepth_ > 0) {
      filterValueDepth_--;
      return EndValue();
    }
    else
      return outputHandler_.EndArray(elementCount) && EndValue();
  }

private:
  FilterKeyHandler(const FilterKeyHandler&);
  FilterKeyHandler& operator=(const FilterKeyHandler&);

  bool EndValue() {
    if (filterValueDepth_ == 1) // Just at the end of value after filtered key
      filterValueDepth_ = 0;
    return true;
  }

  OutputHandler& outputHandler_;
  const char* keyString_;
  const SizeType keyLength_;
  unsigned filterValueDepth_;
  std::stack<SizeType> filteredKeyCount_;
};

// Implements a generator for Document::Populate()
template <typename InputStream>
class FilterKeyReader {
public:
  typedef char Ch;

  FilterKeyReader(InputStream& is, const Ch* keyString, SizeType keyLength) : 
    is_(is), keyString_(keyString), keyLength_(keyLength), parseResult_()
  {}

  // SAX event flow: reader -> filter -> handler
  template <typename Handler>
  bool operator()(Handler& handler) {
    FilterKeyHandler<Handler> filter(handler, keyString_, keyLength_);
    Reader reader;
    parseResult_ = reader.Parse(is_, filter);
    return parseResult_;
  }

  const ParseResult& GetParseResult() const { return parseResult_; }

private:
  FilterKeyReader(const FilterKeyReader&);
  FilterKeyReader& operator=(const FilterKeyReader&);

  InputStream& is_;
  const char* keyString_;
  const SizeType keyLength_;
  ParseResult parseResult_;
};

int main(int argc, char* argv[]) {
  if (argc != 2) {
    fprintf(stderr, "%s input.json\n", argv[0]);
    return 1;
  }

  FILE* fp = fopen(argv[1], "rb");
  if (fp == NULL) {
    cerr << "Unable to open " << argv[1] << endl;
    return 1;
  }

  // Prepare input stream.
  char readBuffer[65536];
  FileReadStream is(fp, readBuffer, sizeof(readBuffer));

  // Prepare Filter
  string key("coordinates");
  FilterKeyReader<FileReadStream> reader(is, key.c_str(), key.size());

  // Populates the filtered events from reader
  Document document;
  document.Populate(reader);
  ParseResult pr = reader.GetParseResult();
  if (!pr) {
    fprintf(stderr, "\nError(%u): %s\n", static_cast<unsigned>(pr.Offset()), GetParseError_En(pr.Code()));
    return 1;
  }

  // Collect stats on geometries
  map<string, int> geometry_counts;
  map<string, int> property_counts;
  int id_counts;
  for (const auto& feature : document["features"].GetArray()) {
    ASSERT(feature.HasMember("geometry"), "feature has no geometry");
    const auto& geometry = feature["geometry"];
    ASSERT(geometry.IsObject(), "geometry is not an object");
    ASSERT(geometry.HasMember("type"), "geometry has no type");
    const auto& type = geometry["type"];
    ASSERT(type.IsString(), "geometry type is not a string");
    geometry_counts[type.GetString()]++;

    if (feature.HasMember("id")) id_counts++;
    if (feature.HasMember("properties")) {
      const auto& props = feature["properties"];
      for (const auto& prop : props.GetObject()) {
        property_counts[prop.name.GetString()] += 1;
      }
    }
  }

  printf("Features: %d\n", document["features"].GetArray().Size());

  printf("Geometries:\n");
  for (const auto type_count : geometry_counts) {
    printf("  %5d: %s\n", type_count.second, type_count.first.c_str());
  }

  printf("Properties:\n");
  for (const auto type_count : property_counts) {
    printf("  %5d: %s\n", type_count.second, type_count.first.c_str());
  }

  // // Prepare JSON writer and output stream.
  // char writeBuffer[65536];
  // FileWriteStream os(stdout, writeBuffer, sizeof(writeBuffer));
  // Writer<FileWriteStream> writer(os);

  // // Write the document to standard output
  // document.Accept(writer);
  return 0;
}
