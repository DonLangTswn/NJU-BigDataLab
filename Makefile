.PHONY: zip
REPORT_DOC=/Users/dlc/Documents/学习/2025大数据实验

zip-%:
	@ cp $(REPORT_DOC)/lab$*/221220070_刁涟承_BG_$*.pdf 221220070_刁涟承_BG_$*.pdf
	@ zip -r "221220070_刁涟承_LAB_$*.zip" \
		./lab$*/ 						\
		221220070_刁涟承_BG_$*.pdf 		 \
	-x 									\
		"*/target/classes/*" 			\
		"*/target/maven-archiver/*" 	\
		"*/target/maven-status/*" 		\
		"*/target/test-classes/*" 		\
		"*.gitignore"
	@ rm 221220070_刁涟承_BG_$*.pdf