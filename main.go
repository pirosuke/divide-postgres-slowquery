package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Songmu/go-httpdate"
)

/*
SlowQuery describes slow query
*/
type SlowQuery struct {
	SQL        string
	LoggedTime string
	Duration   float64
	Params     string
	State      string
	IsEnded    bool
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}

func parseParams(paramString string) map[string]string {
	patternParam := regexp.MustCompile("(\\$[0-9]+) = ([^,]+)")
	paramList := strings.Split(paramString, ",")

	paramMap := map[string]string{}
	for _, param := range paramList {
		m := patternParam.FindStringSubmatch(param)
		if m != nil {
			paramMap[m[1]] = m[2]
		}
	}

	return paramMap
}

func outputSlowQueryFile(outputDirPath string, state SlowQuery) {
	t, err := httpdate.Str2Time(state.LoggedTime, nil)
	if err != nil {
		t = time.Now()
	}

	ts := t.Format("20060102150405")
	outputFilePath := filepath.Join(outputDirPath, ts+".sql")
	fmt.Println("output file: ", outputFilePath)

	paramMap := parseParams(state.Params)

	query := "-- duration: " + strconv.FormatFloat(state.Duration, 'f', 2, 64) + "\n"
	query = query + "explain analyze\n\n"

	query = query + state.SQL
	for paramKey, paramValue := range paramMap {
		re := regexp.MustCompile("([^\\$])(\\" + paramKey + ")([^0-9])")
		query = re.ReplaceAllString(query, "$1"+paramValue+"$3")
	}

	err = ioutil.WriteFile(outputFilePath, []byte(query), 0644)
	if err != nil {
		fmt.Println(err)
	}
}

func main() {
	inputFilePath := flag.String("f", "", "Input File Path")
	outputDirPath := flag.String("o", ".", "Output Dir Path")
	flag.Parse()

	if !fileExists(*outputDirPath) {
		fmt.Println("output dir does not exist")
		return
	}

	patternStart := regexp.MustCompile("(.*)LOG:  duration: (.*) ms  execute <unnamed>: (.*)")
	patternEnd := regexp.MustCompile("(.*)DETAIL:  parameters: (.*)")

	stats := SlowQuery{
		Duration: 0,
		State:    "out",
		IsEnded:  false,
	}

	fp, err := os.Open(*inputFilePath)
	if err != nil {
		fmt.Println("failed opening file.")
		return
	}
	defer fp.Close()

	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		line := scanner.Text()

		if stats.State == "out" {
			m := patternStart.FindStringSubmatch(line)
			if m != nil {
				stats.LoggedTime = m[1]

				duration, _ := strconv.ParseFloat(m[2], 64)
				stats.Duration = duration

				stats.SQL = m[3] + "\n"
				stats.State = "in"
			}
		} else {
			m := patternEnd.FindStringSubmatch(line)
			if m != nil {
				stats.Params = m[2]
				if stats.Duration > 0 {
					outputSlowQueryFile(*outputDirPath, stats)
				}

				stats = SlowQuery{
					Duration: 0,
					State:    "out",
					IsEnded:  false,
				}
			} else {
				stats.SQL += line + "\n"
			}
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
}
