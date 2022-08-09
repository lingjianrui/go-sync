package main

import (
	contract "blockchain/xk/contracts"
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/garyburd/redigo/redis"
	"github.com/spf13/viper"
	_ "github.com/taosdata/driver-go/v2/taosSql"
	"log"
	"math/big"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var providerUrl string
var tableName string
var tag string
var magorToken string
var jobChan = make(chan Job, 1024000)
var blockNumberCache uint64 = 19701602
var blockChan = make(chan *types.Block, 2048000)
var transactionChan = make(chan *TransPair, 2048000)
var existHashListName string
var tdengineHost string
var tdenginePort string
var tdengineUser string
var tdenginePassword string
var contractRepo string
var jobThreadControl chan int
var blockThreadControl chan int
var transactionThreadControl chan int
var createJobDelay int64
var transactionChannelName string
//ERC20通用ABI
var jsonAbi = `
[
{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"constant":true,"inputs":[],"name":"_decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"_name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"_symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"burn","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"getOwner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"mint","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[],"name":"renounceOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"}
]
`

var RedisPool *redis.Pool = &redis.Pool{
	MaxIdle: 50,
	MaxActive: 0,
	IdleTimeout: 100,
	Dial: func()(redis.Conn,error){
		return redis.Dial("tcp","localhost:6379")
	},
}

type Job struct {
	//开始区块
	Begin uint64
	//结束区块
	End uint64
}

type TransPair struct {
	//自定义交易
	CustomTx map[string]interface{}
	//交易本身
	Tx *types.Transaction
	//区块时间戳
	Ts uint64
	//区块号
	Num uint64
}

//DecodeTransactionInputData 解析交易输入数据
func DecodeTransactionInputData(contractABI *abi.ABI, data []byte, st *SyncTransaction) *SyncTransaction {
	methodSigData := data[:4]
	inputsSigData := data[4:]
	method, err := contractABI.MethodById(methodSigData)
	if err != nil {
		log.Println(err)
	}
	inputsMap := make(map[string]interface{})
	if err := method.Inputs.UnpackIntoMap(inputsMap, inputsSigData); err != nil {
		log.Println(err)
		st.Amount = "0"
		st.To = "0x"
	} else {
		st.Amount = inputsMap["amount"].(*big.Int).String()
		st.To = inputsMap["recipient"].(common.Address).Hex()
	}
	return st
}

//GetTransactionMessage 获取交易信息
func GetTransactionMessage(tx *types.Transaction) types.Message {
	msg, err := tx.AsMessage(types.LatestSignerForChainID(tx.ChainId()), nil)
	if err != nil {
		log.Fatal(err)
	}
	return msg
}

type SyncTransaction struct {
	Hash string
	From string
	To   string
	Amount string
	Decimals string
	ContractAddress string
	Name string
	Symbol string
	Gas string
	GasPrice string
	Nonce string
	BlockNumber string
	BlockTmp string
}

//GetContractInfo 获取合约信息
func GetContractInfo(contractAddress string, client *ethclient.Client) (string,string,string) {
	conn := RedisPool.Get()
	defer conn.Close()
	res, _:= conn.Do("HGET", contractRepo, contractAddress)
	if res != nil {
		info := strings.Split(string(res.([]byte)),":")
		return info[0],info[1],info[2]
	}else{
		//log.Println("["+string(res.([]byte))+"] New Token Contract")
		address := common.HexToAddress(contractAddress)
		instance, e := contract.NewContract(address, client)
		name, e := instance.Name(nil)
		if e != nil {
			log.Println(e)
		}
		decimal, e := instance.MDecimals(nil)
		if e != nil {
			log.Println(e)
		}
		symbol, e := instance.Symbol(nil)
		if e != nil {
			log.Println(e)
		}
		conn.Do("HSET", contractRepo, contractAddress, name+":"+symbol+":"+strconv.Itoa(int(decimal)))
		//uint8 转 字符串
		return name,symbol, strconv.FormatInt(int64(decimal), 10)
	}

}

//ParseTransactionBaseInfo 解析交易基本信息
func ParseTransactionBaseInfo(tx *types.Transaction, st *SyncTransaction, isMager bool) *SyncTransaction {
	st.Hash = tx.Hash().Hex()
	st.From = GetTransactionMessage(tx).From().Hex()
	st.Gas = strconv.Itoa(int(tx.Gas()))
	st.GasPrice = tx.GasPrice().String()
	st.Nonce = strconv.Itoa(int(tx.Nonce()))
	if isMager {
		st.ContractAddress = ""
	}else {
		st.ContractAddress = tx.To().Hex()
	}
	return st
}

//DecodeTransactionLogs 解析交易日志
func DecodeTransactionLogs(receipt *types.Receipt, contractABI *abi.ABI) {
	for _, vLog := range receipt.Logs {
		// topic[0] is the event name
		event, err := contractABI.EventByID(vLog.Topics[0])
		if err != nil {
			log.Println(err)
		}
		fmt.Printf("Event Name: %s\n", event.Name)
		// topic[1:] is other indexed params in event
		if len(vLog.Topics) > 1 {
			for i, param := range vLog.Topics[1:] {
				fmt.Printf("Indexed params %d in hex: %s\n", i, param)
				fmt.Printf("Indexed params %d decoded %s\n", i, common.HexToAddress(param.Hex()))
			}
		}
		if len(vLog.Data) > 0 {
			fmt.Printf("Log Data in Hex: %s\n", hex.EncodeToString(vLog.Data))
			outputDataMap := make(map[string]interface{})
			err = contractABI.UnpackIntoMap(outputDataMap, event.Name, vLog.Data)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Event outputs: %v\n", outputDataMap)
		}
	}
}

//GetTransactionReceipt 获取交易回执
func GetTransactionReceipt(client *ethclient.Client, txHash common.Hash) *types.Receipt {
	receipt, err := client.TransactionReceipt(context.Background(), txHash)
	if err != nil {
		log.Println(err)
	}
	return receipt
}

//CreateJob 创建Job
func CreateJob(client *ethclient.Client) {
	var lock sync.Mutex
	//获取链上最新的区块编号
	blockNumber, err := client.BlockNumber(context.Background())
	log.Printf("BlockNumber:[%d] Cache:[%d] jobChan:[%d] \n",blockNumber-1,blockNumberCache, len(jobChan))
	if err != nil {
		log.Println(err)
	}
	if blockNumber-1 > blockNumberCache {
		//创建一个新的job
		newJob := Job{
			Begin: blockNumberCache,
			End:   blockNumber-1,
		}
		//将job放入channel
		jobChan <- newJob
		lock.Lock()
		blockNumberCache = blockNumber - 1
		lock.Unlock()
	}
}

//ProcessJob 处理job
func ProcessJob(client *ethclient.Client) {
	for {
		select {
		case job := <-jobChan:
			log.Printf("ProcessJob:[%d] End:[%d]\n", job.Begin, job.End)
			for i := job.Begin; i < job.End; i++ {
				block, err := client.BlockByNumber(context.Background(), big.NewInt(int64(i)))
				if err != nil {
					log.Fatal(err)
				}
				blockChan <- block
			}
		}
	}
}

//InitDb 初始化数据库
func InitDb() {
	var taosUri = fmt.Sprintf("%s:%s/tcp(%s:%s)/",tdengineUser,tdenginePassword,tdengineHost,tdenginePort)
	//连接Tdengine数据库
	conn, err := sql.Open("taosSql", taosUri)
	if err != nil {
		log.Fatal("Failed to connect TDengine, err:", err)
		return
	}
	defer conn.Close()
	conn.Exec(`CREATE DATABASE IF NOT EXISTS sync precision "ns"`)
	conn.Exec("USE sync")
	conn.Exec("CREATE STABLE IF NOT EXISTS transaction (ts TIMESTAMP, from_address  BINARY(100), to_address BINARY(100), amount BINARY(100),decimals BINARY(10), contract_address BINARY(100), hash BINARY(100), token_name BINARY(100), token_symbol BINARY(100), gas BINARY(100), gasprice BINARY(100), nonce BINARY(100), blockNumber BINARY(100), block_timestamp BINARY(30)) TAGS (blockchain BINARY(10));")
	conn.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s USING transaction TAGS ('%s')",tableName,tag))
}

//LoadConfig 加载配置
func LoadConfig() *ethclient.Client{
	//加载配置文件
	Config := viper.New()
	Config.SetConfigName("config")
	Config.SetConfigType("yaml")            // 配置文件格式
	Config.AddConfigPath(".")
	if err := Config.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Fatal("找不到配置文件")
		} else {
			log.Fatal("配置文件出错")
		}
	}
	tdengineHost = Config.Get("tdengine.host").(string)
	tdenginePort = Config.Get("tdengine.port").(string)
	tdengineUser = Config.Get("tdengine.user").(string)
	tdenginePassword = Config.Get("tdengine.password").(string)
	providerUrl = Config.GetString("provider_url")
	jsonAbi = Config.GetString("erc20_json_abi")
	magorToken = Config.GetString("magor_token")
	tableName = Config.GetString("table_name")
	contractRepo = Config.GetString("contract_repo")
	existHashListName = Config.GetString("exist_hash_list_name")
	client, err := ethclient.Dial(providerUrl)
	if err != nil {
		log.Fatal(err)
	}
	blockNumberCache ,_= client.BlockNumber(context.Background())
	tag = Config.GetString("tag")
	jobThread := Config.GetInt64("job_thread_control")
	blockThread := Config.GetInt64("block_thread_control")
	transactionThread := Config.GetInt64("transaction_thread_control")
	jobThreadControl = make(chan int, jobThread)
	blockThreadControl = make(chan int, blockThread)
	transactionThreadControl = make(chan int, transactionThread)
	createJobDelay = Config.GetInt64("create_job_delay")
	transactionChannelName = Config.GetString("transaction_channel_name")
	return client
}

//ProcessBlock 处理区块chan
func ProcessBlock(client *ethclient.Client) {
	for {
		select {
		case block := <-blockChan:
			blockThreadControl <- 1
			log.Printf("ProcessBlock====================block:%d[%d]================================ \n", block.Number().Uint64(), len(block.Transactions()))
			go func(){
				for _, tx := range block.Transactions() {
					pair := &TransPair{Tx: tx, Ts:block.Time(), Num: block.Number().Uint64()}
					transactionChan <- pair
				}
				<-blockThreadControl
			}()
		}
	}
}

//ProcessTransaction 处理transaction
func ProcessTransaction(client *ethclient.Client, tChan chan *TransPair, chanName string) {
	for {
		select {
		case txPair := <-tChan:
			log.Printf("tChan:[%d] \n",len(tChan))
			transactionThreadControl <- 1
			go func(txPair *TransPair) {
				txHash := txPair.Tx.Hash()
				tx, _, err := client.TransactionByHash(context.Background(), txHash)
				if err != nil {
					log.Fatal(err)
				}
				methodHex := hex.EncodeToString(tx.Data())
				//增加对主代币的处理 原理只要有value信息就认为有主代币的交易
				v := tx.Value()
				if methodHex == "" && v.Cmp(big.NewInt(0)) == 1{
					log.Printf("ProcessTransaction - JobChannel:[%d] BlockChannel:[%d] Block:[%d] Tx:[%s] %s:[%d] %s \n", len(jobChan),len(blockChan), txPair.Num, txPair.Tx.Hash(), chanName, len(tChan), magorToken)
					st := &SyncTransaction{}
					st = ParseTransactionBaseInfo(tx, st, true)
					st.To = tx.To().Hex()
					st.Amount = tx.Value().String()
					st.Name = magorToken
					st.Symbol = magorToken
					st.Decimals = "18"
					st.BlockTmp = strconv.FormatUint(txPair.Ts, 10)
					st.BlockNumber = strconv.FormatUint(txPair.Num, 10)
					//插入数据
					InsertData(tableName, st)
				}
				//判断字符串是否以开头
				if strings.HasPrefix(methodHex, "a9059cbb") {
					log.Printf("JobChannel:[%d] BlockChannel:[%d] Block:[%d] Tx:[%s] %s:[%d] %s \n", len(jobChan),len(blockChan),txPair.Num, txPair.Tx.Hash(), chanName, len(tChan), "ERC20")
					st := &SyncTransaction{}
					st = ParseTransactionBaseInfo(tx, st, false)
					//contractABI := GetContractABI(tx.To().String(), etherscanAPIKEY)
					contractABI, err := abi.JSON(strings.NewReader(jsonAbi))
					if err != nil {
						log.Println(err)
					}
					st = DecodeTransactionInputData(&contractABI, tx.Data(), st)
					name, symbol, decimals := GetContractInfo(st.ContractAddress, client)
					st.Name = name
					st.Symbol = symbol
					st.Decimals = decimals
					st.BlockTmp = strconv.FormatUint(txPair.Ts, 10)
					st.BlockNumber = strconv.FormatUint(txPair.Num, 10)
					//插入数据
					InsertData(tableName, st)
					//receipt := GetTransactionReceipt(client, txHash)
					//DecodeTransactionLogs(receipt, &contractABI)
				}
				<-transactionThreadControl
			}(txPair)

		}
	}
}

//InsertData 插入数据
func InsertData(tableName string, st *SyncTransaction) {
	var taosUri = fmt.Sprintf("%s:%s/tcp(%s:%s)/sync",tdengineUser,tdenginePassword,tdengineHost,tdenginePort)
	//连接Tdengine数据库
	conn, err := sql.Open("taosSql", taosUri)
	if err != nil {
		log.Fatal("Failed to connect TDengine, err:", err)
		return
	}
	defer conn.Close()
	//获取当前13位时间戳
	now := time.Now().UnixNano()
	//查询redis中是否存在
	rc := RedisPool.Get()
	defer rc.Close()
	res, _:= rc.Do("HGET", existHashListName, st.Hash)
	if res != nil {
		log.Println(st.Hash+"记录已经存在")
	}else {
		sql := fmt.Sprintf("insert into %s values(%d,\"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\", \"%s\")",
			tableName,
			now,
			strings.ToLower(st.From),
			strings.ToLower(st.To),
			st.Amount,
			st.Decimals,
			strings.ToLower(st.ContractAddress),
			st.Hash,
			st.Name,
			st.Symbol,
			st.Gas,
			st.GasPrice,
			st.Nonce,
			st.BlockNumber,
			st.BlockTmp,
		)
		//log.Println(sql)
		_, e := conn.Exec(sql)
		if e != nil {
			log.Println(e)
		}
		log.Printf("Block:[%s], Info:[%s] 记录插入成功",st.BlockNumber,st.Hash)
		rc.Do("HSET", contractRepo, existHashListName, st.Hash)
	}
}

func main() {
	runtime.GOMAXPROCS(50)
	client := LoadConfig()
	InitDb()
	wg := &sync.WaitGroup{}
	wg.Add(4)
	//每30秒钟运行一次CreateJob
	go func() {
		for {
			CreateJob(client)
			time.Sleep(time.Duration(createJobDelay) * time.Second)
		}
	}()
	go ProcessJob(client)
	go ProcessBlock(client)
	go ProcessTransaction(client, transactionChan, transactionChannelName)
	wg.Wait()
}


