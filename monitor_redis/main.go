package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/datacratic/gometrics/metric"
	"github.com/datacratic/gometrics/trace"
	"github.com/datacratic/goredis/redis"
	"golang.org/x/net/context"
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

	t := &trace.Periodic{
		Period: 10 * time.Second,
		Handler: &trace.Metrics{
			Prefix: "redis",
			Reporter: &metric.Carbon{
				URLs:   []string{"tcp://" + *graphiteAddress},
				Prefix: *graphitePrefix,
			},
		},
	}

	keyAddressPairs := map[string]string{}
	connections := map[string]*redis.Conn{}

	for i := 0; i < len(addresses); i++ {
		key := addresses[i]
		i++
		address := addresses[i]
		keyAddressPairs[key] = address
		fmt.Println(key, address)

		var err error
		connections[key] = redis.Dial("tcp", address)
		if err != nil {
			panic("connection error to " + key + " " + address)
		}
	}

	last_used_cpu := map[string]map[string]float64{}
	for key, _ := range connections {
		last_used_cpu[key] = map[string]float64{}
	}

	doMonitor := func(ctx context.Context) {
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
					trace.Set(ctx, key+".used_cpu_sys", diff)
				case "used_cpu_user":
					i, err := strconv.ParseFloat(split[1], 64)
					if err != nil {
						fmt.Println("error parse float: " + err.Error())
						break
					}
					diff := i - last_used_cpu[key]["used_cpu_user"]
					last_used_cpu[key]["used_cpu_user"] = i
					trace.Set(ctx, key+".used_cpu_user", diff)
				case "used_memory":
					i, err := strconv.ParseInt(split[1], 10, 64)
					if err != nil {
						fmt.Println("error parse int: " + err.Error())
						break
					}
					trace.Set(ctx, key+".used_memory", i)
				case "connected_clients":
					i, err := strconv.ParseInt(split[1], 10, 64)
					if err != nil {
						fmt.Println("error parse int: " + err.Error())
						break
					}
					trace.Set(ctx, key+".connected_clients", i)
				case "instantaneous_ops_per_sec":
					i, err := strconv.ParseInt(split[1], 10, 64)
					if err != nil {
						fmt.Println("error parse int: " + err.Error())
						break
					}
					trace.Set(ctx, key+".instantaneous_ops_per_sec", i)
				}
			}
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	tick := time.NewTicker(time.Second * 1).C

	for {
		select {
		case <-tick:
			c := trace.Start(trace.SetHandler(context.Background(), t), "monitor", "")
			doMonitor(c)

			trace.Leave(c, "latency_micro_sec")
		case <-sig:
			break
		}
	}

	fmt.Println("shutting down")
}
