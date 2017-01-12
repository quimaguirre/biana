
%s/HREF="reference_manual.html\(.*\)"/HREF="\1"/g
%s/SRC="\(.*\)"/SRC="http:\/\/sbi.imim.es\/web\/images\/manual\/\1"/g
write
quit

