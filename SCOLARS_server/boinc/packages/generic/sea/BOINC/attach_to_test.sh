# !/bin/sh
# --no_gui_rpc

valgrind --leak-check=full --log-file=valgrind_output ./boinc_client -no_gui_rpc --redirectio --dir ./client_testing/ --attach_project http://debian.localdomain/test/ 7612cda51b3a7edaa4d9d98102dfb081
