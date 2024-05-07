#ifndef LOG_LOG_WRITER
#define LOG_LOG_WRITER

#include <string>

class LogWriter {
public:
  LogWriter() = delete;
  LogWriter(std::string dir);
  LogWriter( const LogWriter& ) = delete;
  LogWriter& operator=( const LogWriter& ) = delete;
private:
  std::string dir_;
};

#endif
