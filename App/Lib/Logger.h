#ifndef SGXJOINEVALUATION_LOGGER_H
#define SGXJOINEVALUATION_LOGGER_H

#include "LoggerTypes.h"

void initLogger();

void logger(LEVEL level, const char *fmt, ...);

#endif //SGXJOINEVALUATION_LOGGER_H
