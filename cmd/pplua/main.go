package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
)

var include = regexp.MustCompile(`^\s*\#include\s"([^\"]*)"$`)

func process(w io.Writer, filename string) (err error) {
	if !strings.HasSuffix(filename, ".lua") {
		filename += ".lua"
	}

	file, err := os.Open(filename)
	if err != nil {
		return
	}

	defer file.Close()
	s := bufio.NewScanner(bufio.NewReader(file))

	for s.Scan() {
		matches := include.FindStringSubmatch(s.Text())
		if len(matches) == 2 {
			if err = process(w, matches[1]); err != nil {
				return
			}
		} else {
			fmt.Fprintf(w, "%s\n", s.Text())
		}
	}

	err = s.Err()
	return
}

func main() {
	out := flag.String("o", "", "name of the output file")
	pkg := flag.String("p", "", "package name")
	key := flag.String("v", "", "variable name")

	flag.Parse()

	if *out == "" {
		log.Fatal("missing output filename")
	}

	if *pkg == "" {
		log.Fatal("missing package")
	}

	if *key == "" {
		log.Fatal("missing variable name")
	}

	file, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	fmt.Fprintf(w, "package %s\n\nvar %s = map[string]string{", *pkg, *key)

	for _, item := range flag.Args() {
		fmt.Fprintf(w, "\n\t\"%s\": `\n", item)

		err = process(w, item)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Fprintf(w, "`,\n")
	}

	fmt.Fprintf(w, "}\n")
}
