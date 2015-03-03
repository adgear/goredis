package main

import (
	"flag"
	"fmt"
	"github.com/datacratic/goredis/redis"
	"github.com/rcrowley/go-metrics"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {

	fmt.Println(len(os.Args), os.Args)

	graphiteAddress := flag.String("graphite", "", "address of the graphite to send metrics to")
	graphitePrefix := flag.String("prefix", "", "prefix for graphite keys")
	flag.Parse()
	addresses := flag.Args()

	fmt.Println("graphite:", *graphiteAddress)
	fmt.Println("prefix:", *graphitePrefix)
	fmt.Println("key-address pairs:", addresses)

	if *graphiteAddress == "" {
		panic("Provide a graphite address to save metrics")
	}

	if *graphitePrefix == "" {
		panic("Provide a prefix for graphite keys")
	}

	keyAddressPairs := map[string]string{}
	connections := map[string]redis.Conn{}

	for i := 0; i < len(addresses); i++ {
		key := addresses[i]
		i++
		address := addresses[i]
		keyAddressPairs[key] = address
		fmt.Println(key, address)

		var err error
		connections[key], err = redis.Dial("tcp", address)
		if err != nil {
			panic("connection error to " + key + " " + address)
		}
	}

	registry := metrics.NewRegistry()

	last_used_cpu := map[string]map[string]float64{}
	for key, _ := range connections {
		last_used_cpu[key] = map[string]float64{}

		metrics.NewRegisteredGaugeFloat64(key+".used_cpu_sys", registry)
		metrics.NewRegisteredGaugeFloat64(key+".used_cpu_user", registry)
		metrics.NewRegisteredGauge(key+".used_memory", registry)
		metrics.NewRegisteredGauge(key+".connected_clients", registry)
		metrics.NewRegisteredGauge(key+".instantaneous_ops_per_sec", registry)
	}

	doMonitor := func() {
		for key, conn := range connections {
			info, err := conn.Do("INFO")
			if err != nil {
				fmt.Println(err)
			}
			lines := strings.Split(string(info.([]byte)), "\r\n")
			for _, line := range lines {
				if line == "" || line[0] == '#' {
					continue
				}
				split := strings.Split(line, ":")

				switch split[0] {
				case "used_cpu_sys":
					i, err := strconv.ParseFloat(split[1], 64)
					if err != nil {
						fmt.Println("error parse float: " + err.Error())
						break
					}
					diff := i - last_used_cpu[key]["used_cpu_sys"]
					last_used_cpu[key]["used_cpu_sys"] = i
					if gauge, ok := registry.Get(key + ".used_cpu_sys").(metrics.GaugeFloat64); ok {
						gauge.Update(diff)
					}
				case "used_cpu_user":
					i, err := strconv.ParseFloat(split[1], 64)
					if err != nil {
						fmt.Println("error parse float: " + err.Error())
						break
					}
					diff := i - last_used_cpu[key]["used_cpu_user"]
					last_used_cpu[key]["used_cpu_user"] = i
					if gauge, ok := registry.Get(key + ".used_cpu_user").(metrics.GaugeFloat64); ok {
						gauge.Update(diff)
					}
				case "used_memory":
					i, err := strconv.ParseInt(split[1], 10, 64)
					if err != nil {
						fmt.Println("error parse int: " + err.Error())
						break
					}
					if gauge, ok := registry.Get(key + ".used_memory").(metrics.Gauge); ok {
						gauge.Update(i)
					}
				case "connected_clients":
					i, err := strconv.ParseInt(split[1], 10, 64)
					if err != nil {
						fmt.Println("error parse int: " + err.Error())
						break
					}
					if gauge, ok := registry.Get(key + ".connected_clients").(metrics.Gauge); ok {
						gauge.Update(i)
					}
				case "instantaneous_ops_per_sec":
					i, err := strconv.ParseInt(split[1], 10, 64)
					if err != nil {
						fmt.Println("error parse int: " + err.Error())
						break
					}
					if gauge, ok := registry.Get(key + ".instantaneous_ops_per_sec").(metrics.Gauge); ok {
						gauge.Update(i)
					}
				}
			}
		}
	}

	addr, _ := net.ResolveTCPAddr("tcp", *graphiteAddress)
	fmt.Println("graphite address:", addr)
	go metrics.Graphite(registry, 1*time.Second, *graphitePrefix, addr)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	tickC := time.NewTicker(time.Second * 1).C

	metrics.NewRegisteredGauge("latency_micro_sec", registry)

	for {
		select {
		case <-tickC:
			//fmt.Println(time.Now())
			startTime := time.Now().UnixNano()
			doMonitor()
			endTime := time.Now().UnixNano()
			elapsed := endTime - startTime
			if gauge, ok := registry.Get("latency_micro_sec").(metrics.Gauge); ok {
				gauge.Update(int64(elapsed / 1000))
			}
		case sig := <-sigChan:
			fmt.Println("shutting down", sig)
			os.Exit(0)
			break
		}
	}
}
