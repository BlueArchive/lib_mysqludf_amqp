cmake_minimum_required(VERSION 3.10)
project(test)

set(CMAKE_CXX_STANDARD 14)

include_directories(. ../ /usr/include/mysql/)
link_directories(/usr/lib/mysql/plugin /usr/local/lib ../.libs)

add_executable(test_v2
        test.c
        ../send.c
        /usr/include/mysql/mysql.h
        /usr/include/mysql/mysql_com.h
        ../lib_mysqludf_amqp.h)

target_link_libraries(test_v2 lib_mysqludf_amqp.a /usr/local/lib/librabbitmq.so bsd pthread)

add_executable(test_v1
        test.c
        ../lib_mysqludf_amqp.h)

target_link_libraries(test_v1 lib_mysqludf_amqp.a /usr/local/lib/librabbitmq.so bsd pthread)
