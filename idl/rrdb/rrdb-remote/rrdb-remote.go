// Autogenerated by Thrift Compiler (0.11.0)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/idl/rrdb"
	"github.com/apache/thrift/lib/go/thrift"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var _ = base.GoUnusedProtection__
var _ = replication.GoUnusedProtection__

func Usage() {
	fmt.Fprintln(os.Stderr, "Usage of ", os.Args[0], " [-h host:port] [-u url] [-f[ramed]] function [arg1 [arg2...]]:")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nFunctions:")
	fmt.Fprintln(os.Stderr, "  update_response put(update_request update)")
	fmt.Fprintln(os.Stderr, "  update_response multi_put(multi_put_request request)")
	fmt.Fprintln(os.Stderr, "  update_response remove(blob key)")
	fmt.Fprintln(os.Stderr, "  multi_remove_response multi_remove(multi_remove_request request)")
	fmt.Fprintln(os.Stderr, "  incr_response incr(incr_request request)")
	fmt.Fprintln(os.Stderr, "  check_and_set_response check_and_set(check_and_set_request request)")
	fmt.Fprintln(os.Stderr, "  read_response get(blob key)")
	fmt.Fprintln(os.Stderr, "  multi_get_response multi_get(multi_get_request request)")
	fmt.Fprintln(os.Stderr, "  count_response sortkey_count(blob hash_key)")
	fmt.Fprintln(os.Stderr, "  ttl_response ttl(blob key)")
	fmt.Fprintln(os.Stderr, "  scan_response get_scanner(get_scanner_request request)")
	fmt.Fprintln(os.Stderr, "  scan_response scan(scan_request request)")
	fmt.Fprintln(os.Stderr, "  void clear_scanner(i64 context_id)")
	fmt.Fprintln(os.Stderr)
	os.Exit(0)
}

func main() {
	flag.Usage = Usage
	var host string
	var port int
	var protocol string
	var urlString string
	var framed bool
	var useHttp bool
	var parsedUrl *url.URL
	var trans thrift.TTransport
	_ = strconv.Atoi
	_ = math.Abs
	flag.Usage = Usage
	flag.StringVar(&host, "h", "localhost", "Specify host and port")
	flag.IntVar(&port, "p", 9090, "Specify port")
	flag.StringVar(&protocol, "P", "binary", "Specify the protocol (binary, compact, simplejson, json)")
	flag.StringVar(&urlString, "u", "", "Specify the url")
	flag.BoolVar(&framed, "framed", false, "Use framed transport")
	flag.BoolVar(&useHttp, "http", false, "Use http")
	flag.Parse()

	if len(urlString) > 0 {
		var err error
		parsedUrl, err = url.Parse(urlString)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
			flag.Usage()
		}
		host = parsedUrl.Host
		useHttp = len(parsedUrl.Scheme) <= 0 || parsedUrl.Scheme == "http"
	} else if useHttp {
		_, err := url.Parse(fmt.Sprint("http://", host, ":", port))
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error parsing URL: ", err)
			flag.Usage()
		}
	}

	cmd := flag.Arg(0)
	var err error
	if useHttp {
		trans, err = thrift.NewTHttpClient(parsedUrl.String())
	} else {
		portStr := fmt.Sprint(port)
		if strings.Contains(host, ":") {
			host, portStr, err = net.SplitHostPort(host)
			if err != nil {
				fmt.Fprintln(os.Stderr, "error with host:", err)
				os.Exit(1)
			}
		}
		trans, err = thrift.NewTSocket(net.JoinHostPort(host, portStr))
		if err != nil {
			fmt.Fprintln(os.Stderr, "error resolving address:", err)
			os.Exit(1)
		}
		if framed {
			trans = thrift.NewTFramedTransport(trans)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creating transport", err)
		os.Exit(1)
	}
	defer trans.Close()
	var protocolFactory thrift.TProtocolFactory
	switch protocol {
	case "compact":
		protocolFactory = thrift.NewTCompactProtocolFactory()
		break
	case "simplejson":
		protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
		break
	case "json":
		protocolFactory = thrift.NewTJSONProtocolFactory()
		break
	case "binary", "":
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
		break
	default:
		fmt.Fprintln(os.Stderr, "Invalid protocol specified: ", protocol)
		Usage()
		os.Exit(1)
	}
	iprot := protocolFactory.GetProtocol(trans)
	oprot := protocolFactory.GetProtocol(trans)
	client := rrdb.NewRrdbClient(thrift.NewTStandardClient(iprot, oprot))
	if err := trans.Open(); err != nil {
		fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
		os.Exit(1)
	}

	switch cmd {
	case "put":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "Put requires 1 args")
			flag.Usage()
		}
		arg32 := flag.Arg(1)
		mbTrans33 := thrift.NewTMemoryBufferLen(len(arg32))
		defer mbTrans33.Close()
		_, err34 := mbTrans33.WriteString(arg32)
		if err34 != nil {
			Usage()
			return
		}
		factory35 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt36 := factory35.GetProtocol(mbTrans33)
		argvalue0 := rrdb.NewUpdateRequest()
		err37 := argvalue0.Read(jsProt36)
		if err37 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.Put(context.Background(), value0))
		fmt.Print("\n")
		break
	case "multi_put":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "MultiPut requires 1 args")
			flag.Usage()
		}
		arg38 := flag.Arg(1)
		mbTrans39 := thrift.NewTMemoryBufferLen(len(arg38))
		defer mbTrans39.Close()
		_, err40 := mbTrans39.WriteString(arg38)
		if err40 != nil {
			Usage()
			return
		}
		factory41 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt42 := factory41.GetProtocol(mbTrans39)
		argvalue0 := rrdb.NewMultiPutRequest()
		err43 := argvalue0.Read(jsProt42)
		if err43 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.MultiPut(context.Background(), value0))
		fmt.Print("\n")
		break
	case "remove":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "Remove requires 1 args")
			flag.Usage()
		}
		arg44 := flag.Arg(1)
		mbTrans45 := thrift.NewTMemoryBufferLen(len(arg44))
		defer mbTrans45.Close()
		_, err46 := mbTrans45.WriteString(arg44)
		if err46 != nil {
			Usage()
			return
		}
		factory47 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt48 := factory47.GetProtocol(mbTrans45)
		argvalue0 := base.NewBlob()
		err49 := argvalue0.Read(jsProt48)
		if err49 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.Remove(context.Background(), value0))
		fmt.Print("\n")
		break
	case "multi_remove":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "MultiRemove requires 1 args")
			flag.Usage()
		}
		arg50 := flag.Arg(1)
		mbTrans51 := thrift.NewTMemoryBufferLen(len(arg50))
		defer mbTrans51.Close()
		_, err52 := mbTrans51.WriteString(arg50)
		if err52 != nil {
			Usage()
			return
		}
		factory53 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt54 := factory53.GetProtocol(mbTrans51)
		argvalue0 := rrdb.NewMultiRemoveRequest()
		err55 := argvalue0.Read(jsProt54)
		if err55 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.MultiRemove(context.Background(), value0))
		fmt.Print("\n")
		break
	case "incr":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "Incr requires 1 args")
			flag.Usage()
		}
		arg56 := flag.Arg(1)
		mbTrans57 := thrift.NewTMemoryBufferLen(len(arg56))
		defer mbTrans57.Close()
		_, err58 := mbTrans57.WriteString(arg56)
		if err58 != nil {
			Usage()
			return
		}
		factory59 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt60 := factory59.GetProtocol(mbTrans57)
		argvalue0 := rrdb.NewIncrRequest()
		err61 := argvalue0.Read(jsProt60)
		if err61 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.Incr(context.Background(), value0))
		fmt.Print("\n")
		break
	case "check_and_set":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "CheckAndSet requires 1 args")
			flag.Usage()
		}
		arg62 := flag.Arg(1)
		mbTrans63 := thrift.NewTMemoryBufferLen(len(arg62))
		defer mbTrans63.Close()
		_, err64 := mbTrans63.WriteString(arg62)
		if err64 != nil {
			Usage()
			return
		}
		factory65 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt66 := factory65.GetProtocol(mbTrans63)
		argvalue0 := rrdb.NewCheckAndSetRequest()
		err67 := argvalue0.Read(jsProt66)
		if err67 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.CheckAndSet(context.Background(), value0))
		fmt.Print("\n")
		break
	case "get":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "Get requires 1 args")
			flag.Usage()
		}
		arg68 := flag.Arg(1)
		mbTrans69 := thrift.NewTMemoryBufferLen(len(arg68))
		defer mbTrans69.Close()
		_, err70 := mbTrans69.WriteString(arg68)
		if err70 != nil {
			Usage()
			return
		}
		factory71 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt72 := factory71.GetProtocol(mbTrans69)
		argvalue0 := base.NewBlob()
		err73 := argvalue0.Read(jsProt72)
		if err73 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.Get(context.Background(), value0))
		fmt.Print("\n")
		break
	case "multi_get":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "MultiGet requires 1 args")
			flag.Usage()
		}
		arg74 := flag.Arg(1)
		mbTrans75 := thrift.NewTMemoryBufferLen(len(arg74))
		defer mbTrans75.Close()
		_, err76 := mbTrans75.WriteString(arg74)
		if err76 != nil {
			Usage()
			return
		}
		factory77 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt78 := factory77.GetProtocol(mbTrans75)
		argvalue0 := rrdb.NewMultiGetRequest()
		err79 := argvalue0.Read(jsProt78)
		if err79 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.MultiGet(context.Background(), value0))
		fmt.Print("\n")
		break
	case "sortkey_count":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "SortkeyCount requires 1 args")
			flag.Usage()
		}
		arg80 := flag.Arg(1)
		mbTrans81 := thrift.NewTMemoryBufferLen(len(arg80))
		defer mbTrans81.Close()
		_, err82 := mbTrans81.WriteString(arg80)
		if err82 != nil {
			Usage()
			return
		}
		factory83 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt84 := factory83.GetProtocol(mbTrans81)
		argvalue0 := base.NewBlob()
		err85 := argvalue0.Read(jsProt84)
		if err85 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.SortkeyCount(context.Background(), value0))
		fmt.Print("\n")
		break
	case "ttl":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "TTL requires 1 args")
			flag.Usage()
		}
		arg86 := flag.Arg(1)
		mbTrans87 := thrift.NewTMemoryBufferLen(len(arg86))
		defer mbTrans87.Close()
		_, err88 := mbTrans87.WriteString(arg86)
		if err88 != nil {
			Usage()
			return
		}
		factory89 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt90 := factory89.GetProtocol(mbTrans87)
		argvalue0 := base.NewBlob()
		err91 := argvalue0.Read(jsProt90)
		if err91 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.TTL(context.Background(), value0))
		fmt.Print("\n")
		break
	case "get_scanner":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "GetScanner requires 1 args")
			flag.Usage()
		}
		arg92 := flag.Arg(1)
		mbTrans93 := thrift.NewTMemoryBufferLen(len(arg92))
		defer mbTrans93.Close()
		_, err94 := mbTrans93.WriteString(arg92)
		if err94 != nil {
			Usage()
			return
		}
		factory95 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt96 := factory95.GetProtocol(mbTrans93)
		argvalue0 := rrdb.NewGetScannerRequest()
		err97 := argvalue0.Read(jsProt96)
		if err97 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.GetScanner(context.Background(), value0))
		fmt.Print("\n")
		break
	case "scan":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "Scan requires 1 args")
			flag.Usage()
		}
		arg98 := flag.Arg(1)
		mbTrans99 := thrift.NewTMemoryBufferLen(len(arg98))
		defer mbTrans99.Close()
		_, err100 := mbTrans99.WriteString(arg98)
		if err100 != nil {
			Usage()
			return
		}
		factory101 := thrift.NewTSimpleJSONProtocolFactory()
		jsProt102 := factory101.GetProtocol(mbTrans99)
		argvalue0 := rrdb.NewScanRequest()
		err103 := argvalue0.Read(jsProt102)
		if err103 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.Scan(context.Background(), value0))
		fmt.Print("\n")
		break
	case "clear_scanner":
		if flag.NArg()-1 != 1 {
			fmt.Fprintln(os.Stderr, "ClearScanner requires 1 args")
			flag.Usage()
		}
		argvalue0, err104 := (strconv.ParseInt(flag.Arg(1), 10, 64))
		if err104 != nil {
			Usage()
			return
		}
		value0 := argvalue0
		fmt.Print(client.ClearScanner(context.Background(), value0))
		fmt.Print("\n")
		break
	case "":
		Usage()
		break
	default:
		fmt.Fprintln(os.Stderr, "Invalid function ", cmd)
	}
}
