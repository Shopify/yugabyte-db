#pragma once

#include <string>

#include "opentelemetry/trace/provider.h"

namespace yb {
namespace util {

class OpenTelemetry {
public:
    static void Init();
    static void Shutdown();
    static opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>
        GetTracer(const std::string& name);
private:
};

} // namespace util
} // namespace yb
