# !/bin/sh
# --no_gui_rpc

valgrind --leak-check=full --log-file=valgrind_output ./boinc_client -no_gui_rpc --redirectio --dir ./2nd_client/ --attach_project http://debian.localdomain/test/ 81372fe1a8658efb38b731b11cbc3b27
