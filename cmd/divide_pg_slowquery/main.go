package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

/*
Config describes divide config setting.
*/
type Config struct {
	PatternStart string `json:"pattern_start"`
	PatternEnd   string `json:"pattern_end"`
}

/*
SlowQuery describes slow query.
*/
type SlowQuery struct {
	SQL          string
	LoggedTime   string
	Duration     float64
	Params       string
	IsInProgress bool
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
	//t, err := httpdate.Str2Time(state.LoggedTime, "2006-01-02 15:04:05")
	t, err := time.Parse("2006-01-02 15:04:05.000 MST", state.LoggedTime)
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

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: divide_pg_slowquery [flags]\n")
		flag.PrintDefaults()
	}

	inputFilePath := flag.String("f", "", "Input File Path")
	outputDirBasePath := flag.String("o", ".", "Output Dir Path")
	configFilePath := flag.String("c", "", "Config File Path")
	flag.Parse()

	if !fileExists(*outputDirBasePath) {
		fmt.Println("output dir does not exist")
		return
	}

	config := new(Config)

	if *configFilePath != "" {
		if !fileExists(*configFilePath) {
			fmt.Println("config file does not exist")
			return
		}

		jsonContent, err := ioutil.ReadFile(*configFilePath)
		if err != nil {
			fmt.Println("failed to read config file: " + *configFilePath)
			return
		}

		if err := json.Unmarshal(jsonContent, config); err != nil {
			fmt.Println("failed to read config file: " + *configFilePath)
			return
		}
	} else {
		config.PatternStart = "^\\[([^\\]]*)\\].*LOG:  duration: (.*) ms  execute <unnamed>: (.*)"
		config.PatternEnd = "(.*)DETAIL:  parameters: (.*)"
	}

	t := time.Now()
	ts := t.Format("20060102150405")
	outputDirPath := filepath.Join(*outputDirBasePath, ts)
	os.MkdirAll(outputDirPath, os.ModePerm)

	patternStart := regexp.MustCompile(config.PatternStart)
	patternEnd := regexp.MustCompile(config.PatternEnd)

	stats := SlowQuery{
		Duration:     0,
		IsInProgress: false,
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

		if !stats.IsInProgress {
			m := patternStart.FindStringSubmatch(line)
			if m != nil {
				stats.LoggedTime = m[1]

				duration, _ := strconv.ParseFloat(m[2], 64)
				stats.Duration = duration

				stats.SQL = m[3] + "\n"
				stats.IsInProgress = true
			}
		} else {
			m := patternEnd.FindStringSubmatch(line)
			if m != nil {
				stats.Params = m[2]
				if stats.Duration > 0 {
					outputSlowQueryFile(outputDirPath, stats)
				}

				stats = SlowQuery{
					Duration:     0,
					IsInProgress: false,
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
