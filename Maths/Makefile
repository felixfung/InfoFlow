DOC = infomap_math

INCLUDE = $(wildcard *.tex)

.PHONY: release
default: $(DOC).pdf

$(DOC).pdf: $(INCLUDE)
	pdflatex $(DOC)

release: $(INCLUDE)
	pdflatex $(DOC)
	bibtex $(DOC)
	pdflatex $(DOC)
	pdflatex $(DOC)
