#include "blocking_java_generator.h"
#include "rsocket/options.pb.h"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <vector>
#include <google/protobuf/compiler/java/java_names.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>

// Stringify helpers used solely to cast RSOCKET_RPC_VERSION
#ifndef STR
#define STR(s) #s
#endif

#ifndef XSTR
#define XSTR(s) STR(s)
#endif

#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED
#endif

namespace blocking_java_rsocket_rpc_generator {

using google::protobuf::FileDescriptor;
using google::protobuf::ServiceDescriptor;
using google::protobuf::MethodDescriptor;
using google::protobuf::Descriptor;
using google::protobuf::io::Printer;
using google::protobuf::SourceLocation;
using io::rsocket::rpc::RSocketMethodOptions;

// Adjust a method name prefix identifier to follow the JavaBean spec:
//   - decapitalize the first letter
//   - remove embedded underscores & capitalize the following letter
static string MixedLower(const string& word) {
  string w;
  w += tolower(word[0]);
  bool after_underscore = false;
  for (size_t i = 1; i < word.length(); ++i) {
    if (word[i] == '_') {
      after_underscore = true;
    } else {
      w += after_underscore ? toupper(word[i]) : word[i];
      after_underscore = false;
    }
  }
  return w;
}

// Converts to the identifier to the ALL_UPPER_CASE format.
//   - An underscore is inserted where a lower case letter is followed by an
//     upper case letter.
//   - All letters are converted to upper case
static string ToAllUpperCase(const string& word) {
  string w;
  for (size_t i = 0; i < word.length(); ++i) {
    w += toupper(word[i]);
    if ((i < word.length() - 1) && islower(word[i]) && isupper(word[i + 1])) {
      w += '_';
    }
  }
  return w;
}

static inline string LowerMethodName(const MethodDescriptor* method) {
  return MixedLower(method->name());
}

static inline string MethodFieldName(const MethodDescriptor* method) {
  return "METHOD_" + ToAllUpperCase(method->name());
}

static inline string RouteFieldName(const MethodDescriptor* method) {
  return "ROUTE_" + ToAllUpperCase(method->name());
}

static inline string MessageFullJavaName(const Descriptor* desc) {
  return google::protobuf::compiler::java::ClassName(desc);
}

static inline string ServiceFieldName(const ServiceDescriptor* service) { return "SERVICE_ID"; }

static inline string NamespaceIdFieldName(const ServiceDescriptor* service) { return "NAMESPACE_ID"; }

template <typename ITR>
static void SplitStringToIteratorUsing(const string& full,
                                       const char* delim,
                                       ITR& result) {
  // Optimize the common case where delim is a single character.
  if (delim[0] != '\0' && delim[1] == '\0') {
    char c = delim[0];
    const char* p = full.data();
    const char* end = p + full.size();
    while (p != end) {
      if (*p == c) {
        ++p;
      } else {
        const char* start = p;
        while (++p != end && *p != c);
        *result++ = string(start, p - start);
      }
    }
    return;
  }

  string::size_type begin_index, end_index;
  begin_index = full.find_first_not_of(delim);
  while (begin_index != string::npos) {
    end_index = full.find_first_of(delim, begin_index);
    if (end_index == string::npos) {
      *result++ = full.substr(begin_index);
      return;
    }
    *result++ = full.substr(begin_index, (end_index - begin_index));
    begin_index = full.find_first_not_of(delim, end_index);
  }
}

static void SplitStringUsing(const string& full,
                             const char* delim,
                             std::vector<string>* result) {
  std::back_insert_iterator< std::vector<string> > it(*result);
  SplitStringToIteratorUsing(full, delim, it);
}

static std::vector<string> Split(const string& full, const char* delim) {
  std::vector<string> result;
  SplitStringUsing(full, delim, &result);
  return result;
}

static string EscapeJavadoc(const string& input) {
  string result;
  result.reserve(input.size() * 2);

  char prev = '*';

  for (string::size_type i = 0; i < input.size(); i++) {
    char c = input[i];
    switch (c) {
      case '*':
        // Avoid "/*".
        if (prev == '/') {
          result.append("&#42;");
        } else {
          result.push_back(c);
        }
        break;
      case '/':
        // Avoid "*/".
        if (prev == '*') {
          result.append("&#47;");
        } else {
          result.push_back(c);
        }
        break;
      case '@':
        // '@' starts javadoc tags including the @deprecated tag, which will
        // cause a compile-time error if inserted before a declaration that
        // does not have a corresponding @Deprecated annotation.
        result.append("&#64;");
        break;
      case '<':
        // Avoid interpretation as HTML.
        result.append("&lt;");
        break;
      case '>':
        // Avoid interpretation as HTML.
        result.append("&gt;");
        break;
      case '&':
        // Avoid interpretation as HTML.
        result.append("&amp;");
        break;
      case '\\':
        // Java interprets Unicode escape sequences anywhere!
        result.append("&#92;");
        break;
      default:
        result.push_back(c);
        break;
    }

    prev = c;
  }

  return result;
}

template <typename DescriptorType>
static string GetCommentsForDescriptor(const DescriptorType* descriptor) {
  SourceLocation location;
  if (descriptor->GetSourceLocation(&location)) {
    return location.leading_comments.empty() ?
      location.trailing_comments : location.leading_comments;
  }
  return string();
}

static std::vector<string> GetDocLines(const string& comments) {
  if (!comments.empty()) {
    // Ideally we should parse the comment text as Markdown and
    // write it back as HTML, but this requires a Markdown parser.  For now
    // we just use <pre> to get fixed-width text formatting.

    // If the comment itself contains block comment start or end markers,
    // HTML-escape them so that they don't accidentally close the doc comment.
    string escapedComments = EscapeJavadoc(comments);

    std::vector<string> lines = Split(escapedComments, "\n");
    while (!lines.empty() && lines.back().empty()) {
      lines.pop_back();
    }
    return lines;
  }
  return std::vector<string>();
}

template <typename DescriptorType>
static std::vector<string> GetDocLinesForDescriptor(const DescriptorType* descriptor) {
  return GetDocLines(GetCommentsForDescriptor(descriptor));
}

static void WriteDocCommentBody(Printer* printer,
                                    const std::vector<string>& lines,
                                    bool surroundWithPreTag) {
  if (!lines.empty()) {
    if (surroundWithPreTag) {
      printer->Print(" * <pre>\n");
    }

    for (size_t i = 0; i < lines.size(); i++) {
      // Most lines should start with a space.  Watch out for lines that start
      // with a /, since putting that right after the leading asterisk will
      // close the comment.
      if (!lines[i].empty() && lines[i][0] == '/') {
        printer->Print(" * $line$\n", "line", lines[i]);
      } else {
        printer->Print(" *$line$\n", "line", lines[i]);
      }
    }

    if (surroundWithPreTag) {
      printer->Print(" * </pre>\n");
    }
  }
}

static void WriteDocComment(Printer* printer, const string& comments) {
  printer->Print("/**\n");
  std::vector<string> lines = GetDocLines(comments);
  WriteDocCommentBody(printer, lines, false);
  printer->Print(" */\n");
}

static void WriteServiceDocComment(Printer* printer,
                                       const ServiceDescriptor* service) {
  // Deviating from protobuf to avoid extraneous docs
  // (see https://github.com/google/protobuf/issues/1406);
  printer->Print("/**\n");
  std::vector<string> lines = GetDocLinesForDescriptor(service);
  WriteDocCommentBody(printer, lines, true);
  printer->Print(" */\n");
}

void WriteMethodDocComment(Printer* printer,
                           const MethodDescriptor* method) {
  // Deviating from protobuf to avoid extraneous docs
  // (see https://github.com/google/protobuf/issues/1406);
  printer->Print("/**\n");
  std::vector<string> lines = GetDocLinesForDescriptor(method);
  WriteDocCommentBody(printer, lines, true);
  printer->Print(" */\n");
}

static void PrintInterface(const ServiceDescriptor* service,
                           std::map<string, string>* vars,
                           Printer* p,
                           ProtoFlavor flavor,
                           bool disable_version) {
  (*vars)["service_name"] = service->name();
  (*vars)["service_field_name"] = ServiceFieldName(service);
  (*vars)["file_name"] = service->file()->name();
  (*vars)["RSOCKET_RPC_VERSION"] = "";
  #ifdef RSOCKET_RPC_VERSION
  if (!disable_version) {
    (*vars)["RSOCKET_RPC_VERSION"] = " (version " XSTR(RSOCKET_RPC_VERSION) ")";
  }
  #endif
  WriteServiceDocComment(p, service);
  p->Print(
      *vars,
      "@$Generated$(\n"
      "    value = \"by RSocket RPC proto compiler$RSOCKET_RPC_VERSION$\",\n"
      "    comments = \"Source: $file_name$\")\n"
      "public interface Blocking$service_name$ {\n");
  p->Indent();

  // Service IDs
  p->Print(*vars, "String $service_field_name$ = \"$Package$$service_name$\";\n");

  for (int i = 0; i < service->method_count(); ++i) {
    const MethodDescriptor* method = service->method(i);
    (*vars)["method_field_name"] = MethodFieldName(method);
    (*vars)["route_field_name"] = RouteFieldName(method);
    (*vars)["method_name"] = method->name();

    p->Print(*vars, "String $method_field_name$ = \"$method_name$\";\n");
    p->Print(*vars, "String $route_field_name$ = $service_field_name$ + \".\" + $method_field_name$;\n");
  }

  // RPC methods
  for (int i = 0; i < service->method_count(); ++i) {
    const MethodDescriptor* method = service->method(i);
    const RSocketMethodOptions options = method->options().GetExtension(io::rsocket::rpc::options);
    (*vars)["input_type"] = MessageFullJavaName(method->input_type());
    (*vars)["output_type"] = MessageFullJavaName(method->output_type());
    (*vars)["lower_method_name"] = LowerMethodName(method);
    bool client_streaming = method->client_streaming();
    bool server_streaming = method->server_streaming();

    // Method signature
    p->Print("\n");
    WriteMethodDocComment(p, method);

    if (server_streaming) {
      p->Print(*vars, "$Iterable$<$output_type$> $lower_method_name$");
    } else if (client_streaming) {
      p->Print(*vars, "$output_type$ $lower_method_name$");
    } else {
      if (options.fire_and_forget()) {
        p->Print(*vars, "void $lower_method_name$");
      } else {
        p->Print(*vars, "$output_type$ $lower_method_name$");
      }
    }
    if (client_streaming) {
      // Bidirectional streaming or client streaming
      p->Print(*vars, "($Iterable$<$input_type$> messages, $ByteBuf$ metadata);\n");
    } else {
      // Server streaming or simple RPC
      p->Print(*vars, "($input_type$ message, $ByteBuf$ metadata);\n");
    }
  }

  p->Outdent();
  p->Print("}\n");
}

static void PrintClient(const ServiceDescriptor* service,
                        std::map<string, string>* vars,
                        Printer* p,
                        ProtoFlavor flavor,
                        bool disable_version) {
  (*vars)["service_name"] = service->name();
  (*vars)["namespace_id_name"] = NamespaceIdFieldName(service);
  (*vars)["service_id_name"] = ServiceFieldName(service);
  (*vars)["file_name"] = service->file()->name();
  (*vars)["client_class_name"] = ClientClassName(service);
  (*vars)["RSOCKET_RPC_VERSION"] = "";
  #ifdef RSOCKET_RPC_VERSION
  if (!disable_version) {
    (*vars)["RSOCKET_RPC_VERSION"] = " (version " XSTR(RSOCKET_RPC_VERSION) ")";
  }
  #endif
  p->Print(
      *vars,
      "@$Generated$(\n"
      "    value = \"by RSocket RPC proto compiler$RSOCKET_RPC_VERSION$\",\n"
      "    comments = \"Source: $file_name$\")\n"
      "@$RSocketRpcGenerated$(\n"
      "    type = $RSocketRpcResourceType$.CLIENT,\n"
      "    idlClass = Blocking$service_name$.class)\n"
      "public final class Blocking$client_class_name$ implements Blocking$service_name$ {\n");
  p->Indent();

  p->Print(
      *vars,
      "private final $PackageName$.$client_class_name$ delegate;\n");
  // RSocket only
  p->Print(
      *vars,
      "\n"
      "public Blocking$client_class_name$($RSocket$ rSocket) {\n");
  p->Indent();
  p->Print(
      *vars,
      "this.delegate = new $PackageName$.$client_class_name$(rSocket);\n");

  p->Outdent();
  p->Print("}\n\n");

  // RSocket And Encoder
  p->Print(
      *vars,
      "\n"
      "public Blocking$client_class_name$($RSocket$ rSocket, $MetadataEncoder$ metadataEncoder) {\n");
  p->Indent();
  p->Print(
      *vars,
      "this.delegate = new $PackageName$.$client_class_name$(rSocket, metadataEncoder);\n");

  p->Outdent();
  p->Print("}\n\n");

  // RSocket and Metrics
  p->Print(
      *vars,
      "public Blocking$client_class_name$($RSocket$ rSocket, $MeterRegistry$ registry) {\n");
  p->Indent();
  p->Print(
      *vars,
      "this.delegate = new $PackageName$.$client_class_name$(rSocket, registry);\n");

  p->Outdent();
  p->Print("}\n\n");

  // RSocket and Encoder and Metrics
  p->Print(
      *vars,
      "public Blocking$client_class_name$($RSocket$ rSocket, $MetadataEncoder$ metadataEncoder, $MeterRegistry$ registry) {\n");
  p->Indent();
  p->Print(
      *vars,
      "this.delegate = new $PackageName$.$client_class_name$(rSocket, metadataEncoder, registry);\n");

  p->Outdent();
  p->Print("}\n\n");

  // RPC methods
  for (int i = 0; i < service->method_count(); ++i) {
    const MethodDescriptor* method = service->method(i);
    const RSocketMethodOptions options = method->options().GetExtension(io::rsocket::rpc::options);
    (*vars)["input_type"] = MessageFullJavaName(method->input_type());
    (*vars)["output_type"] = MessageFullJavaName(method->output_type());
    (*vars)["lower_method_name"] = LowerMethodName(method);
    (*vars)["method_id_name"] = MethodFieldName(method);
    bool client_streaming = method->client_streaming();
    bool server_streaming = method->server_streaming();

    // Method signature
    if (server_streaming) {
      p->Print(
          *vars,
          "@$RSocketRpcGeneratedMethod$(returnTypeClass = $output_type$.class)\n"
          "public $BlockingIterable$<$output_type$> $lower_method_name$");
    } else if (client_streaming) {
      p->Print(
          *vars,
          "@$RSocketRpcGeneratedMethod$(returnTypeClass = $output_type$.class)\n"
          "public $output_type$ $lower_method_name$");
    } else {
      if (options.fire_and_forget()) {
        p->Print(
            *vars,
            "@$RSocketRpcGeneratedMethod$(returnTypeClass = Void.class)\n"
            "public void $lower_method_name$");
      } else {
        p->Print(
            *vars,
            "@$RSocketRpcGeneratedMethod$(returnTypeClass = $output_type$.class)\n"
            "public $output_type$ $lower_method_name$");
      }
    }

    if (client_streaming) {
      p->Print(
          *vars,
          "($Iterable$<$input_type$> messages) {\n");
      p->Indent();
      if (options.fire_and_forget()) {
          p->Print(
              *vars,
              "$lower_method_name$(messages, $Unpooled$.EMPTY_BUFFER);\n");
      } else {
          p->Print(
              *vars,
              "return $lower_method_name$(messages, $Unpooled$.EMPTY_BUFFER);\n");
      }
      p->Outdent();
      p->Print("}\n\n");
    } else {
      // Server streaming or simple RPC
      p->Print(
          *vars,
          "($input_type$ message) {\n");
      p->Indent();
      if (options.fire_and_forget()) {
          p->Print(
              *vars,
              "$lower_method_name$(message, $Unpooled$.EMPTY_BUFFER);\n");
      } else {
          p->Print(
              *vars,
              "return $lower_method_name$(message, $Unpooled$.EMPTY_BUFFER);\n");
      }
      p->Outdent();
      p->Print("}\n\n");
    }

    // Method signature
    if (server_streaming) {
      p->Print(
          *vars,
          "@$Override$\n"
          "@$RSocketRpcGeneratedMethod$(returnTypeClass = $output_type$.class)\n"
          "public $BlockingIterable$<$output_type$> $lower_method_name$");
    } else if (client_streaming) {
      p->Print(
          *vars,
          "@$Override$\n"
          "@$RSocketRpcGeneratedMethod$(returnTypeClass = $output_type$.class)\n"
          "public $output_type$ $lower_method_name$");
    } else {
      const Descriptor* output_type = method->output_type();
      if (options.fire_and_forget()) {
        p->Print(
            *vars,
            "@$Override$\n"
            "@$RSocketRpcGeneratedMethod$(returnTypeClass = Void.class)\n"
            "public void $lower_method_name$");
      } else {
        p->Print(
            *vars,
            "@$Override$\n"
            "@$RSocketRpcGeneratedMethod$(returnTypeClass = $output_type$.class)\n"
            "public $output_type$ $lower_method_name$");
      }
    }

    if (client_streaming && server_streaming) {
      p->Print(
          *vars,
      "($Iterable$<$input_type$> messages, $ByteBuf$ metadata) {\n");
      p->Indent();
      p->Print(
          *vars,
          "$Flux$ stream = delegate.$lower_method_name$($Flux$.defer(() -> $Flux$.fromIterable(messages)), metadata);\n");
      p->Print(
         *vars,
         "return new $BlockingIterable$<>(stream, $Queues$.SMALL_BUFFER_SIZE, $Queues$.small());\n");
      p->Outdent();
      p->Print("}\n\n");
    } else if (server_streaming) {
      p->Print(
          *vars,
          "($input_type$ message, $ByteBuf$ metadata) {\n");
      p->Indent();
      p->Print(
          *vars,
          "$Flux$ stream = delegate.$lower_method_name$(message, metadata);\n");
      p->Print(
          *vars,
          "return new $BlockingIterable$<>(stream, $Queues$.SMALL_BUFFER_SIZE, $Queues$.small());\n");
      p->Outdent();
      p->Print("}\n\n");
    } else if (client_streaming) {
      p->Print(
          *vars,
          "($Iterable$<$input_type$> messages, $ByteBuf$ metadata) {\n");
      p->Indent();
      if (options.fire_and_forget()) {
        p->Print(
            *vars,
            "delegate.$lower_method_name$($Flux$.defer(() -> $Flux$.fromIterable(messages)), metadata).block();\n");
      } else {
        p->Print(
            *vars,
            "return delegate.$lower_method_name$($Flux$.defer(() -> $Flux$.fromIterable(messages)), metadata).block();\n");
      }
      p->Outdent();
      p->Print("}\n\n");
    } else {
      p->Print(
          *vars,
          "($input_type$ message, $ByteBuf$ metadata) {\n");
      p->Indent();
      if (options.fire_and_forget()) {
        p->Print(
            *vars,
            "delegate.$lower_method_name$(message, metadata).block();\n");
      } else {
        p->Print(
            *vars,
            "return delegate.$lower_method_name$(message, metadata).block();\n");
      }
      p->Outdent();
      p->Print("}\n\n");
    }
  }

  p->Outdent();
  p->Print("}\n\n");
}

static void PrintServer(const ServiceDescriptor* service,
                        std::map<string, string>* vars,
                        Printer* p,
                        ProtoFlavor flavor,
                        bool disable_version) {
  (*vars)["service_name"] = service->name();
  (*vars)["namespace_id_name"] = NamespaceIdFieldName(service);
  (*vars)["service_id_name"] = ServiceFieldName(service);
  (*vars)["file_name"] = service->file()->name();
  (*vars)["server_class_name"] = ServerClassName(service);
  (*vars)["RSOCKET_RPC_VERSION"] = "";
  #ifdef RSOCKET_RPC_VERSION
  if (!disable_version) {
    (*vars)["RSOCKET_RPC_VERSION"] = " (version " XSTR(RSOCKET_RPC_VERSION) ")";
  }
  #endif
  p->Print(
      *vars,
      "@$Generated$(\n"
      "    value = \"by RSocket RPC proto compiler$RSOCKET_RPC_VERSION$\",\n"
      "    comments = \"Source: $file_name$\")\n"
      "@$RSocketRpcGenerated$(\n"
      "    type = $RSocketRpcResourceType$.SERVICE,\n"
      "    idlClass = Blocking$service_name$.class)\n"
      "@$Named$(\n"
      "    value =\"Blocking$server_class_name$\")\n"
      "public final class Blocking$server_class_name$ extends $AbstractRSocketService$ {\n");
  p->Indent();

  p->Print(
      *vars,
      "private final Blocking$service_name$ service;\n"
      "private final $MetadataDecoder$ metadataDecoder;\n"
      "private final $Scheduler$ scheduler;\n");

  // RPC metrics
  for (int i = 0; i < service->method_count(); ++i) {
    const MethodDescriptor* method = service->method(i);
    const RSocketMethodOptions options = method->options().GetExtension(io::rsocket::rpc::options);
    (*vars)["lower_method_name"] = LowerMethodName(method);
    bool client_streaming = method->client_streaming();
    bool server_streaming = method->server_streaming();

    if (server_streaming) {
      p->Print(
          *vars,
          "private final $Function$<? super $Publisher$<$Payload$>, ? extends $Publisher$<$Payload$>> $lower_method_name$;\n");
    } else if (client_streaming) {
      p->Print(
          *vars,
          "private final $Function$<? super $Publisher$<$Payload$>, ? extends $Publisher$<$Payload$>> $lower_method_name$;\n");
    } else {
      if (options.fire_and_forget()) {
        p->Print(
            *vars,
            "private final $Function$<? super $Publisher$<Void>, ? extends $Publisher$<Void>> $lower_method_name$;\n");
      } else {
        p->Print(
            *vars,
            "private final $Function$<? super $Publisher$<$Payload$>, ? extends $Publisher$<$Payload$>> $lower_method_name$;\n");
      }
    }
  }

  p->Print(
      *vars,
      "@$Inject$\n"
      "public Blocking$server_class_name$(Blocking$service_name$ service, $Optional$<$MetadataDecoder$> metadataDecoder, $Optional$<$Scheduler$> scheduler, $Optional$<$MeterRegistry$> registry) {\n");
  p->Indent();
  p->Print(
      *vars,
      "this.scheduler = scheduler.orElse($Schedulers$.elastic());\n"
      "this.service = service;\n");
  p->Print(
        *vars,
        "if (!registry.isPresent()) {\n"
    );
    p->Indent();
     // RPC metrics
    for (int i = 0; i < service->method_count(); ++i) {
      const MethodDescriptor* method = service->method(i);
      (*vars)["lower_method_name"] = LowerMethodName(method);

      p->Print(
         *vars,
         "this.$lower_method_name$ = $Function$.identity();\n");
    }

    p->Outdent();
    p->Print(
        *vars,
        "} else {\n"
    );
    p->Indent();
    // RPC metrics
    for (int i = 0; i < service->method_count(); ++i) {
      const MethodDescriptor* method = service->method(i);
      (*vars)["lower_method_name"] = LowerMethodName(method);
      (*vars)["method_field_name"] = MethodFieldName(method);

      p->Print(
          *vars,
          "this.$lower_method_name$ = $RSocketRpcMetrics$.timed(registry.get(), \"rsocket.server\", \"service\", Blocking$service_name$.$service_id_name$, \"method\", Blocking$service_name$.$method_field_name$);\n");
    }

    p->Outdent();
    p->Print("}\n\n");

  // if metadataDecoder present {
    p->Print(
        *vars,
        "if (metadataDecoder.isPresent()) {\n"
    );
    p->Indent();
    p->Print(
       *vars,
       "this.metadataDecoder = metadataDecoder.get();\n");
    p->Outdent();
    p->Print("} else {\n");
    p->Indent();
    p->Print(
       *vars,
       "this.metadataDecoder = new $CompositeMetadataDecoder$();\n");
    p->Outdent();
    p->Print("}\n");
  // }

  p->Outdent();
  p->Print("}\n\n");

  p->Print(
      *vars,
      "@$Override$\n"
      "public String getService() {\n");
  p->Indent();
  p->Print(
      *vars,
      "return Blocking$service_name$.$service_id_name$;\n");
  p->Outdent();
  p->Print("}\n\n");

  p->Print(
        *vars,
        "@$Override$\n"
        "public Class<?> getServiceClass() {\n");
    p->Indent();
    p->Print(
        *vars,
        "return service.getClass();\n");
    p->Outdent();
    p->Print("}\n\n");

  std::vector<const MethodDescriptor*> fire_and_forget;
  std::vector<const MethodDescriptor*> request_response;
  std::vector<const MethodDescriptor*> request_stream;
  std::vector<const MethodDescriptor*> request_channel;

  for (int i = 0; i < service->method_count(); ++i) {
    const MethodDescriptor* method = service->method(i);
    const RSocketMethodOptions options = method->options().GetExtension(io::rsocket::rpc::options);
    bool client_streaming = method->client_streaming();
    bool server_streaming = method->server_streaming();

    if (client_streaming) {
      request_channel.push_back(method);
    } else if (server_streaming) {
      request_stream.push_back(method);
    } else {
      if (options.fire_and_forget()) {
        fire_and_forget.push_back(method);
      } else {
        request_response.push_back(method);
      }
    }
  }

  // Fire and forget
  p->Print(
    *vars,
    "@$Override$\n"
    "public $Mono$<$Void$> fireAndForget($Payload$ payload) {\n");
  p->Indent();
  if (fire_and_forget.empty()) {
    p->Print(
        *vars,
        "return $Mono$.error(new UnsupportedOperationException(\"Fire And Forget is not implemented.\"));\n");
  } else {
    p->Print(
      *vars,
      "try {\n");
    p->Indent();
    p->Print(
      *vars,
      "$Mono$<$Void$> response = metadataDecoder.decode(payload, this::doDecodeAndHandleFireAndForget);\n\n"
      "payload.release();\n\n"
      "return response;\n");
    p->Outdent();
    p->Print(
      *vars,
      "} catch (Throwable t) {\n");
    p->Indent();
    p->Print(
      *vars,
      "payload.release();\n"
      "return $Mono$.error(t);\n");
    p->Outdent();
    p->Print(
      *vars,
     "}\n");
  }
  p->Outdent();
  p->Print(
    *vars,
   "}\n\n");


  // Do Decode And Fire and forget delegate
  p->Print(
    *vars,
    "$Mono$<Void> doDecodeAndHandleFireAndForget(\n");
  p->Indent();
  p->Print(
    *vars,
    "$ByteBuf$ data,\n"
    "$ByteBuf$ metadata,\n"
    "$String$ route,\n"
    "$SpanContext$ spanContext\n");
  p->Outdent();
  p->Print(
      *vars,
      ") throws $Exception$ {\n");
  p->Indent();
  p->Print(
      *vars,
      "switch(route) {\n");
  p->Indent();
  for (vector<const MethodDescriptor*>::iterator it = fire_and_forget.begin(); it != fire_and_forget.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["input_type"] = MessageFullJavaName(method->input_type());
    (*vars)["method_name"] = method->name();
    (*vars)["route_field_name"] = RouteFieldName(method);
    p->Print(
        *vars,
        "case $service_name$.$route_field_name$: {\n");
    p->Indent();
    p->Print(
        *vars,
        "return this.do$method_name$FireAndForget(data, metadata, spanContext);\n");
    p->Outdent();
    p->Print("}\n");
  }
  p->Print(
      *vars,
      "default: {\n");
  p->Indent();
  p->Print(
      *vars,
      "return $Mono$.error(new UnsupportedOperationException());\n");
  p->Outdent();
  p->Print("}\n");
  p->Outdent();
  p->Print("}\n");
  p->Outdent();
  p->Print("}\n\n");


  // Do Service Fire-And-Forget
  for (vector<const MethodDescriptor*>::iterator it = fire_and_forget.begin(); it != fire_and_forget.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["input_type"] = MessageFullJavaName(method->input_type());
    (*vars)["output_type"] = MessageFullJavaName(method->output_type());
    (*vars)["method_name"] = method->name();
    (*vars)["lower_method_name"] = LowerMethodName(method);

    p->Print(
        *vars,
        "private $Mono$<$Void$> do$method_name$FireAndForget($ByteBuf$ data, $ByteBuf$ metadata, $SpanContext$ spanContext) throws $Exception$ {\n"
    );
    p->Indent();
    p->Print(
        *vars,
        "$CodedInputStream$ is = $CodedInputStream$.newInstance(data.nioBuffer());\n"
        "$input_type$ message = $input_type$.parseFrom(is);\n"
        "return $Mono$.<Void>fromRunnable(() -> service.$lower_method_name$(message, metadata)).subscribeOn(scheduler);\n");
    p->Outdent();
    p->Print("}\n");
    p->Print("\n");
  }

  // Request-Response
  p->Print(
      *vars,
      "@$Override$\n"
      "public $Mono$<$Payload$> requestResponse($Payload$ payload) {\n");
  p->Indent();
  if (request_response.empty()) {
    p->Print(
        *vars,
        "return $Mono$.error(new UnsupportedOperationException(\"Request Response is not implemented.\"));\n");
  } else {
    p->Print(
      *vars,
      "try {\n");
    p->Indent();
    p->Print(
      *vars,
      "$Mono$<$Payload$> response = metadataDecoder.decode(payload, this::doDecodeAndHandleRequestResponse);\n\n"
      "payload.release();\n\n"
      "return response;\n");
    p->Outdent();
    p->Print(
      *vars,
      "} catch (Throwable t) {\n");
    p->Indent();
    p->Print(
      *vars,
      "payload.release();\n"
      "return $Mono$.error(t);\n");
    p->Outdent();
    p->Print(
      *vars,
     "}\n");
  }
  p->Outdent();
  p->Print(
    *vars,
   "}\n\n");

  // Do Decode And Request Response delegate
  p->Print(
    *vars,
    "$Mono$<$Payload$> doDecodeAndHandleRequestResponse(\n");
  p->Indent();
  p->Print(
    *vars,
    "$ByteBuf$ data,\n"
    "$ByteBuf$ metadata,\n"
    "$String$ route,\n"
    "$SpanContext$ spanContext\n");
  p->Outdent();
  p->Print(
      *vars,
      ") throws $Exception$ {\n");
  p->Indent();
  p->Print(
      *vars,
      "switch(route) {\n");
  p->Indent();
  for (vector<const MethodDescriptor*>::iterator it = request_response.begin(); it != request_response.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["input_type"] = MessageFullJavaName(method->input_type());
    (*vars)["method_name"] = method->name();
    (*vars)["route_field_name"] = RouteFieldName(method);
    p->Print(
        *vars,
        "case $service_name$.$route_field_name$: {\n");
    p->Indent();
    p->Print(
        *vars,
        "return this.do$method_name$RequestResponse(data, metadata, spanContext);\n");
    p->Outdent();
    p->Print("}\n");
  }
  p->Print(
      *vars,
      "default: {\n");
  p->Indent();
  p->Print(
      *vars,
      "return $Mono$.error(new UnsupportedOperationException());\n");
  p->Outdent();
  p->Print("}\n");
  p->Outdent();
  p->Print("}\n");
  p->Outdent();
  p->Print("}\n\n");

  // Do Request-Response
  for (vector<const MethodDescriptor*>::iterator it = request_response.begin(); it != request_response.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["input_type"] = MessageFullJavaName(method->input_type());
    (*vars)["output_type"] = MessageFullJavaName(method->output_type());
    (*vars)["method_name"] = method->name();
    (*vars)["lower_method_name"] = LowerMethodName(method);

    p->Print(
        *vars,
        "private $Mono$<$Payload$> do$method_name$RequestResponse($ByteBuf$ data, $ByteBuf$ metadata, $SpanContext$ spanContext) throws $Exception$ {\n"
    );
    p->Indent();
    p->Print(
        *vars,
        "$CodedInputStream$ is = $CodedInputStream$.newInstance(data.nioBuffer());\n"
        "$input_type$ message = $input_type$.parseFrom(is);\n"
        "return $Mono$.fromSupplier(() -> service.$lower_method_name$(message, metadata)).map(serializer).transform($lower_method_name$).subscribeOn(scheduler);\n");
    p->Outdent();
    p->Print("}\n");
    p->Print("\n");
  }


  // Request-Stream
  p->Print(
      *vars,
      "@$Override$\n"
      "public $Flux$<$Payload$> requestStream($Payload$ payload) {\n");
  p->Indent();
  if (request_stream.empty()) {
    p->Print(
        *vars,
        "return $Flux$.error(new UnsupportedOperationException(\"Request Stream is not implemented.\"));\n");
  } else {
    p->Print(
      *vars,
      "try {\n");
    p->Indent();
    p->Print(
      *vars,
      "$Flux$<$Payload$> response = metadataDecoder.decode(payload, this::doDecodeAndHandleRequestStream);\n\n"
      "payload.release();\n\n"
      "return response;\n");
    p->Outdent();
    p->Print(
      *vars,
      "} catch (Throwable t) {\n");
    p->Indent();
    p->Print(
      *vars,
      "payload.release();\n"
      "return $Flux$.error(t);\n");
    p->Outdent();
    p->Print(
      *vars,
     "}\n");
  }
  p->Outdent();
  p->Print(
    *vars,
   "}\n\n");

  // Do Decode And Request Stream delegate
  p->Print(
    *vars,
    "$Flux$<$Payload$> doDecodeAndHandleRequestStream(\n");
  p->Indent();
  p->Print(
    *vars,
    "$ByteBuf$ data,\n"
    "$ByteBuf$ metadata,\n"
    "$String$ route,\n"
    "$SpanContext$ spanContext\n");
  p->Outdent();
  p->Print(
      *vars,
      ") throws $Exception$ {\n");
  p->Indent();
  p->Print(
      *vars,
      "switch(route) {\n");
  p->Indent();
  for (vector<const MethodDescriptor*>::iterator it = request_stream.begin(); it != request_stream.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["input_type"] = MessageFullJavaName(method->input_type());
    (*vars)["method_name"] = method->name();
    (*vars)["route_field_name"] = RouteFieldName(method);
    p->Print(
        *vars,
        "case $service_name$.$route_field_name$: {\n");
    p->Indent();
    p->Print(
        *vars,
        "return this.do$method_name$RequestStream(data, metadata, spanContext);\n");
    p->Outdent();
    p->Print("}\n");
  }
  p->Print(
      *vars,
      "default: {\n");
  p->Indent();
  p->Print(
      *vars,
      "return $Flux$.error(new UnsupportedOperationException());\n");
  p->Outdent();
  p->Print("}\n");
  p->Outdent();
  p->Print("}\n");
  p->Outdent();
  p->Print("}\n\n");

  // Do Service Request-Stream
  for (vector<const MethodDescriptor*>::iterator it = request_stream.begin(); it != request_stream.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["input_type"] = MessageFullJavaName(method->input_type());
    (*vars)["output_type"] = MessageFullJavaName(method->output_type());
    (*vars)["method_name"] = method->name();
    (*vars)["lower_method_name"] = LowerMethodName(method);

    p->Print(
        *vars,
        "private $Flux$<$Payload$> do$method_name$RequestStream($ByteBuf$ data, $ByteBuf$ metadata, $SpanContext$ spanContext) throws $Exception$ {\n"
    );
    p->Indent();
    p->Print(
        *vars,
        "$CodedInputStream$ is = $CodedInputStream$.newInstance(data.nioBuffer());\n"
        "$input_type$ message = $input_type$.parseFrom(is);\n"
                  "return $Flux$.defer(() -> $Flux$.fromIterable(service.$lower_method_name$(message, metadata)).map(serializer).transform($lower_method_name$)).subscribeOn(scheduler);\n");
    p->Outdent();
    p->Print("}\n");
    p->Print("\n");
  }

  // Request-Channel
  p->Print(
      *vars,
      "@$Override$\n"
      "public $Flux$<$Payload$> requestChannel($Payload$ payload, $Publisher$<$Payload$> payloads) {\n");
  p->Indent();
  if (request_channel.empty()) {
    p->Print(
        *vars,
        "return $Flux$.error(new UnsupportedOperationException(\"Request Channel is not implemented.\"));\n");
  } else {
    p->Print(
      *vars,
      "try {\n");
    p->Indent();
    p->Print(
      *vars,
      "$Flux$<$Payload$> response = metadataDecoder.decode(payload, ($ByteBuf$ data, $ByteBuf$ metadata, $String$ route, $SpanContext$ spanContext) -> {\n");
    p->Indent();

    p->Print(
        *vars,
        "switch(route) {\n");
    p->Indent();
    for (vector<const MethodDescriptor*>::iterator it = request_channel.begin(); it != request_channel.end(); ++it) {
      const MethodDescriptor* method = *it;
      (*vars)["input_type"] = MessageFullJavaName(method->input_type());
      (*vars)["method_name"] = method->name();
      (*vars)["route_field_name"] = RouteFieldName(method);
      p->Print(
          *vars,
          "case $service_name$.$route_field_name$: {\n");
      p->Indent();
      p->Print(
          *vars,
          "return this.do$method_name$RequestChannel($Flux$.from(payloads), data, metadata, spanContext);\n");
      p->Outdent();
      p->Print("}\n");
    }
    p->Print(
        *vars,
        "default: {\n");
    p->Indent();
    p->Print(
        *vars,
        "payload.release();\n"
        "return $Flux$.error(new UnsupportedOperationException());\n");
    p->Outdent();
    p->Print("}\n");
    p->Outdent();
    p->Print("}\n");
    p->Outdent();
    p->Print("});\n\n");
    p->Print("return response;\n");
    p->Outdent();
    p->Print(
      *vars,
      "} catch (Throwable t) {\n");
    p->Indent();
    p->Print(
      *vars,
      "payload.release();\n"
      "return $Flux$.error(t);\n");
    p->Outdent();
    p->Print(
      *vars,
     "}\n");
  }
  p->Outdent();
  p->Print(
    *vars,
   "}\n\n");

  p->Print(
      *vars,
      "@$Override$\n"
      "public $Flux$<$Payload$> requestChannel($Publisher$<$Payload$> payloads) {\n");
  p->Indent();
  if (request_channel.empty()) {
    p->Print(
        *vars,
        "return $Flux$.error(new UnsupportedOperationException(\"Request-Channel not implemented.\"));\n");
  } else {
    p->Print(
        *vars,
        "return $Flux$.from(payloads).switchOnFirst(new $BiFunction$<$Signal$<? extends $Payload$>, $Flux$<$Payload$>, $Publisher$<? extends $Payload$>>() {\n");
    p->Indent();
    p->Print(
        *vars,
        "@$Override$\n"
        "public $Publisher$<$Payload$> apply($Signal$<? extends $Payload$> payloadSignal, $Flux$<$Payload$> publisher) {\n");
    p->Indent();
    p->Print(
        *vars,
        "if (payloadSignal.hasValue()) {\n");
    p->Indent();
    p->Print(
            *vars,
            "return requestChannel(payloadSignal.get(), publisher);\n");
    p->Outdent();
    p->Print(
            *vars,
            "} else {\n");
    p->Indent();
    p->Print("return publisher;\n");
    p->Outdent();
    p->Print("}\n");
    p->Outdent();
    p->Print("}\n");
    p->Outdent();
    p->Print("});\n");
  }
  p->Outdent();
  p->Print("}\n\n");

  // Do Request-Channel
  for (vector<const MethodDescriptor*>::iterator it = request_channel.begin(); it != request_channel.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["input_type"] = MessageFullJavaName(method->input_type());
    (*vars)["output_type"] = MessageFullJavaName(method->output_type());
    (*vars)["method_name"] = method->name();
    (*vars)["lower_method_name"] = LowerMethodName(method);

    p->Print(
        *vars,
        "private $Flux$<$Payload$> do$method_name$RequestChannel($Flux$<$Payload$> publisher, $ByteBuf$ data, $ByteBuf$ metadata, $SpanContext$ spanContext) throws $Exception$ {\n"
    );
    p->Indent();
    p->Print(
        *vars,
        "$Flux$<$input_type$> messages =\n");
    p->Indent();
    p->Print(
        *vars,
        "publisher.map(deserializer($input_type$.parser()));\n");
    p->Outdent();
    if (method->server_streaming()) {
      p->Print(
          *vars,
          "return $Flux$.defer(() -> $Flux$.fromIterable(service.$lower_method_name$(messages.toIterable(), metadata)).map(serializer).transform($lower_method_name$)).subscribeOn(scheduler);\n");
    } else {
      p->Print(
          *vars,
          "return $Mono$.fromSupplier(() -> service.$lower_method_name$(messages.toIterable(), metadata)).map(serializer).transform($lower_method_name$).$flux$().subscribeOn(scheduler);\n");
    }
    p->Outdent();
    p->Print("}\n");
    p->Print("\n");
  }

  // Self Registration

  p->Print(
    *vars,
    "@$Override$\n"
    "public void selfRegister($Map$<$String$, $IPCFunction$<$Mono$<$Void$>>> fireAndForgetRegistry, $Map$<$String$, $IPCFunction$<$Mono$<$Payload$>>> requestResponseRegistry, $Map$<$String$, $IPCFunction$<$Flux$<$Payload$>>> requestStreamRegistry, $Map$<$String$, $IPCChannelFunction$> requestChannelRegistry) {\n");
  p->Indent();
  for (vector<const MethodDescriptor*>::iterator it = fire_and_forget.begin(); it != fire_and_forget.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["method_name"] = method->name();
    (*vars)["route_field_name"] = RouteFieldName(method);
    p->Print(
        *vars,
        "fireAndForgetRegistry.put($service_name$.$route_field_name$, this::do$method_name$FireAndForget);\n");
  }
  for (vector<const MethodDescriptor*>::iterator it = request_response.begin(); it != request_response.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["method_name"] = method->name();
    (*vars)["route_field_name"] = RouteFieldName(method);
    p->Print(
        *vars,
        "requestResponseRegistry.put($service_name$.$route_field_name$, this::do$method_name$RequestResponse);\n");
  }
  for (vector<const MethodDescriptor*>::iterator it = request_stream.begin(); it != request_stream.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["method_name"] = method->name();
    (*vars)["route_field_name"] = RouteFieldName(method);
    p->Print(
        *vars,
        "requestStreamRegistry.put($service_name$.$route_field_name$, this::do$method_name$RequestStream);\n");
  }
  for (vector<const MethodDescriptor*>::iterator it = request_channel.begin(); it != request_channel.end(); ++it) {
    const MethodDescriptor* method = *it;
    (*vars)["method_name"] = method->name();
    (*vars)["route_field_name"] = RouteFieldName(method);
    p->Print(
        *vars,
        "requestChannelRegistry.put($service_name$.$route_field_name$, this::do$method_name$RequestChannel);\n");
  }
  p->Outdent();
  p->Print("}\n");
  p->Print("\n");

  // Serializer
  p->Print(
      *vars,
      "private static final $Function$<$MessageLite$, $Payload$> serializer =\n");
  p->Indent();
  p->Print(
      *vars,
      "new $Function$<$MessageLite$, $Payload$>() {\n");
  p->Indent();
  p->Print(
      *vars,
      "@$Override$\n"
      "public $Payload$ apply($MessageLite$ message) {\n");
  p->Indent();
  p->Print(
    *vars,
    "int length = message.getSerializedSize();\n"
    "$ByteBuf$ byteBuf = $ByteBufAllocator$.DEFAULT.buffer(length);\n");
  p->Print("try {\n");
  p->Indent();
  p->Print(
    *vars,
    "message.writeTo($CodedOutputStream$.newInstance(byteBuf.internalNioBuffer(0, length)));\n"
    "byteBuf.writerIndex(length);\n"
    "return $ByteBufPayload$.create(byteBuf);\n");
  p->Outdent();
  p->Print("} catch (Throwable t) {\n");
  p->Indent();
  p->Print(
    "byteBuf.release();\n"
    "throw new RuntimeException(t);\n");
  p->Outdent();
  p->Print("}\n");
  p->Outdent();
  p->Print("}\n");
  p->Outdent();
  p->Print("};\n\n");
  p->Outdent();

  // Deserializer
  p->Print(
      *vars,
      "private static <T> $Function$<$Payload$, T> deserializer(final $Parser$<T> parser) {\n");
  p->Indent();
  p->Print(
      *vars,
      "return new $Function$<$Payload$, T>() {\n");
  p->Indent();
  p->Print(
      *vars,
      "@$Override$\n"
      "public T apply($Payload$ payload) {\n");
  p->Indent();
  p->Print(
      *vars,
      "try {\n");
  p->Indent();
  p->Print(
      *vars,
      "$CodedInputStream$ is = $CodedInputStream$.newInstance(payload.getData());\n"
      "return parser.parseFrom(is);\n");
  p->Outdent();
  p->Print("} catch (Throwable t) {\n");
  p->Indent();
  p->Print(
      *vars,
      "throw new RuntimeException(t);\n");
  p->Outdent();
  p->Print("} finally {\n");
  p->Indent();
  p->Print("payload.release();\n");
  p->Outdent();
  p->Print("}\n");
  p->Outdent();
  p->Print("}\n");
  p->Outdent();
  p->Print("};\n");
  p->Outdent();
  p->Print("}\n");

  p->Outdent();
  p->Print("}\n");
}

void GenerateInterface(const ServiceDescriptor* service,
                       google::protobuf::io::ZeroCopyOutputStream* out,
                       ProtoFlavor flavor,
                       bool disable_version) {
  // All non-generated classes must be referred by fully qualified names to
  // avoid collision with generated classes.
  std::map<string, string> vars;
  vars["Generated"] = "javax.annotation.Generated";
  vars["ByteBuf"] = "io.netty.buffer.ByteBuf";
  vars["Iterable"] = "Iterable";

  Printer printer(out, '$');
    string package_name = ServiceJavaPackage(service->file());
    if (!package_name.empty()) {
      printer.Print(
          "package $package_name$;\n\n",
          "package_name", package_name);
    }

    // Package string is used to fully qualify method names.
    vars["Package"] = service->file()->package();
    if (!vars["Package"].empty()) {
      vars["Package"].append(".");
    }
    PrintInterface(service, &vars, &printer, flavor, disable_version);
}

void GenerateClient(const ServiceDescriptor* service,
                    google::protobuf::io::ZeroCopyOutputStream* out,
                    ProtoFlavor flavor,
                    bool disable_version) {
  // All non-generated classes must be referred by fully qualified names to
  // avoid collision with generated classes.
  std::map<string, string> vars;
  vars["Flux"] = "reactor.core.publisher.Flux";
  vars["Mono"] = "reactor.core.publisher.Mono";
  vars["from"] = "from";
  vars["Function"] = "java.util.function.Function";
  vars["Supplier"] = "java.util.function.Supplier";
  vars["AtomicBoolean"] = "java.util.concurrent.atomic.AtomicBoolean";
  vars["Override"] = "java.lang.Override";
  vars["Publisher"] = "org.reactivestreams.Publisher";
  vars["Generated"] = "javax.annotation.Generated";
  vars["RSocketRpcGenerated"] = "io.rsocket.rpc.annotations.internal.Generated";
  vars["RSocketRpcResourceType"] = "io.rsocket.rpc.annotations.internal.ResourceType";
  vars["RSocket"] = "io.rsocket.RSocket";
  vars["Payload"] = "io.rsocket.Payload";
  vars["ByteBufPayload"] = "io.rsocket.util.ByteBufPayload";
  vars["ByteBuf"] = "io.netty.buffer.ByteBuf";
  vars["ByteBufAllocator"] = "io.netty.buffer.ByteBufAllocator";
  vars["Unpooled"] = "io.netty.buffer.Unpooled";
  vars["ByteBuffer"] = "java.nio.ByteBuffer";
  vars["CodedInputStream"] = "com.google.protobuf.CodedInputStream";
  vars["CodedOutputStream"] = "com.google.protobuf.CodedOutputStream";
  vars["RSocketRpcMetadata"] = "io.rsocket.rpc.frames.Metadata";
  vars["RSocketRpcMetrics"] = "io.rsocket.rpc.metrics.Metrics";
  vars["MeterRegistry"] = "io.micrometer.core.instrument.MeterRegistry";
  vars["MessageLite"] = "com.google.protobuf.MessageLite";
  vars["Parser"] = "com.google.protobuf.Parser";
  vars["BlockingIterable"] = " io.rsocket.rpc.BlockingIterable";
  vars["Iterable"] = "Iterable";
  vars["PackageName"] = ServiceJavaPackage(service->file());
  vars["Queues"] = "reactor.util.concurrent.Queues";
  vars["RSocketRpcGeneratedMethod"] = "io.rsocket.rpc.annotations.internal.GeneratedMethod";
  vars["Tag"] = "io.rsocket.rpc.tracing.Tag";
  vars["Map"] = "java.util.Map";
  vars["HashMap"] = "java.util.HashMap";
  vars["Supplier"] = "java.util.function.Supplier";
  vars["MetadataEncoder"] = "io.rsocket.ipc.MetadataEncoder";
  vars["BackwardCompatibleMetadataEncoder"] = "io.rsocket.ipc.encoders.BackwardCompatibleMetadataEncoder";
  vars["SimpleSpanContext"] = "io.rsocket.ipc.tracing.SimpleSpanContext";

  Printer printer(out, '$');
    string package_name = ServiceJavaPackage(service->file());
    if (!package_name.empty()) {
      printer.Print(
          "package $package_name$;\n\n",
          "package_name", package_name);
    }

    // Package string is used to fully qualify method names.
    vars["Package"] = service->file()->package();
    if (!vars["Package"].empty()) {
      vars["Package"].append(".");
    }
    PrintClient(service, &vars, &printer, flavor, disable_version);
}

void GenerateServer(const ServiceDescriptor* service,
                    google::protobuf::io::ZeroCopyOutputStream* out,
                    ProtoFlavor flavor,
                    bool disable_version) {
  // All non-generated classes must be referred by fully qualified names to
  // avoid collision with generated classes.
  std::map<string, string> vars;
  vars["Flux"] = "reactor.core.publisher.Flux";
  vars["Mono"] = "reactor.core.publisher.Mono";
  vars["from"] = "from";
  vars["flux"] = "flux";
  vars["flatMap"] = "flatMapMany";
  vars["Function"] = "java.util.function.Function";
  vars["Supplier"] = "java.util.function.Supplier";
  vars["BiFunction"] = "java.util.function.BiFunction";
  vars["Override"] = "java.lang.Override";
  vars["Publisher"] = "org.reactivestreams.Publisher";
  vars["Generated"] = "javax.annotation.Generated";
  vars["RSocketRpcGenerated"] = "io.rsocket.rpc.annotations.internal.Generated";
  vars["RSocketRpcResourceType"] = "io.rsocket.rpc.annotations.internal.ResourceType";
  vars["RSocket"] = "io.rsocket.RSocket";
  vars["Payload"] = "io.rsocket.Payload";
  vars["ByteBufPayload"] = "io.rsocket.util.ByteBufPayload";
  vars["SwitchTransformFlux"] = "io.rsocket.internal.SwitchTransformFlux";
  vars["AbstractRSocketService"] = "io.rsocket.rpc.AbstractRSocketService";
  vars["RSocketRpcMetadata"] = "io.rsocket.rpc.frames.Metadata";
  vars["RSocketRpcMetrics"] = "io.rsocket.rpc.metrics.Metrics";
  vars["MeterRegistry"] = "io.micrometer.core.instrument.MeterRegistry";
  vars["ByteBuf"] = "io.netty.buffer.ByteBuf";
  vars["ByteBuffer"] = "java.nio.ByteBuffer";
  vars["ByteBufAllocator"] = "io.netty.buffer.ByteBufAllocator";
  vars["CodedInputStream"] = "com.google.protobuf.CodedInputStream";
  vars["CodedOutputStream"] = "com.google.protobuf.CodedOutputStream";
  vars["MessageLite"] = "com.google.protobuf.MessageLite";
  vars["Parser"] = "com.google.protobuf.Parser";
  vars["BlockingIterable"] = " io.rsocket.rpc.BlockingIterable";
  vars["Iterable"] = "Iterable";
  vars["Scheduler"] = "reactor.core.scheduler.Scheduler";
  vars["Schedulers"] = "reactor.core.scheduler.Schedulers";
  vars["Optional"] = "java.util.Optional";
  vars["Inject"] = "javax.inject.Inject";
  vars["Named"] = "javax.inject.Named";
  vars["RSocketRpcResourceType"] = "io.rsocket.rpc.annotations.internal.ResourceType";
  vars["Tag"] = "io.rsocket.rpc.tracing.Tag";
  vars["SpanContext"] = "io.opentracing.SpanContext";
  vars["Tracer"] = "io.opentracing.Tracer";
  vars["Map"] = "java.util.Map";
  vars["IPCFunction"] = "io.rsocket.ipc.util.IPCFunction";
  vars["IPCChannelFunction"] = "io.rsocket.ipc.util.IPCChannelFunction";
  vars["String"] = "java.lang.String";
  vars["Void"] = "java.lang.Void";
  vars["Signal"] = "reactor.core.publisher.Signal";
  vars["Exception"] = "java.lang.Exception";
  vars["MetadataDecoder"] = "io.rsocket.ipc.MetadataDecoder";
  vars["CompositeMetadataDecoder"] = "io.rsocket.ipc.decoders.CompositeMetadataDecoder";

  Printer printer(out, '$');
    string package_name = ServiceJavaPackage(service->file());
    if (!package_name.empty()) {
      printer.Print(
          "package $package_name$;\n\n",
          "package_name", package_name);
    }

    // Package string is used to fully qualify method names.
    vars["Package"] = service->file()->package();
    if (!vars["Package"].empty()) {
      vars["Package"].append(".");
    }
    PrintServer(service, &vars, &printer, flavor, disable_version);
}

string ServiceJavaPackage(const FileDescriptor* file) {
  string result = google::protobuf::compiler::java::ClassName(file);
  size_t last_dot_pos = result.find_last_of('.');
  if (last_dot_pos != string::npos) {
    result.resize(last_dot_pos);
  } else {
    result = "";
  }
  return result;
}

string ClientClassName(const google::protobuf::ServiceDescriptor* service) {
  return service->name() + "Client";
}

string ServerClassName(const google::protobuf::ServiceDescriptor* service) {
  return service->name() + "Server";
}

}  // namespace java_rsocket_rpc_generator
