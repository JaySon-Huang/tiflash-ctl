package pg

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
)

var (
	txnCommands = []struct {
		Name string
		Cmd  string
	}{
		{"R_CUST", "SELECT C_CUSTKEY FROM HAT.CUSTOMER WHERE C_NAME = ?"},
		{"R_PART", "SELECT P_PRICE FROM HAT.PART WHERE P_PARTKEY = ?"},
		{"R_SUPP", "SELECT S_SUPPKEY FROM HAT.SUPPLIER WHERE S_NAME = ?"},
		{"R_DATE", "SELECT D_DATEKEY FROM HAT.DATE WHERE D_DATE = ?"},

		// New order transaction's commands
		{"C_ORDR", `INSERT INTO HAT.LINEORDER(
	LO_ORDERKEY, LO_LINENUMBER, LO_CUSTKEY, LO_PARTKEY, LO_SUPPKEY, LO_ORDERDATE, 
	LO_ORDPRIORITY, LO_SHIPPRIORITY, LO_QUANTITY, LO_EXTENDEDPRICE, LO_DISCOUNT, LO_REVENUE,
	LO_SUPPLYCOST, LO_TAX, LO_COMMITDATE, LO_SHIPMODE)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`},

		// Payment transaction's commands
		{"R_NATI", "SELECT C_CUSTKEY, C_NAME FROM HAT.CUSTOMER WHERE C_NATION = ?"},
		{"U_CUST", `UPDATE HAT.CUSTOMER SET C_PAYMENTCNT = 0 WHERE C_CUSTKEY = ?`},
		{"U_SUPP", `UPDATE HAT.SUPPLIER SET S_YTD = S_YTD - ? WHERE S_SUPPKEY = ?`},
		{"C_HIST", `INSERT INTO HAT.HISTORY(H_ORDERKEY, H_CUSTKEY, H_AMOUNT) VALUES(?,?,?)`},

		// Count orders transaction's commands
		{"R_ORDR", `SELECT COUNT(DISTINCT LO_ORDERKEY) FROM HAT.LINEORDER WHERE LO_CUSTKEY = ?`},
	}

	txnQueries = []string{
		// New order transaction's commands
		"SELECT HAT.NEWORDER($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17);",
		// Payment transaction
		"SELECT HAT.PAYMENT(?,?,?,?,?,?);",
		// Count orders transaction's commands
		"SELECT * FROM HAT.COUNTORDERS(?,?,?);",
	}
)

type TxnClient struct {
	conn         *pgx.Conn
	dataSource   *DataSource
	numClients   int
	loOrderKey   int
	localCounter int
}

func NewTxnClient(ctx context.Context, connString string, dataSrc *DataSource) (*TxnClient, error) {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &TxnClient{
		conn:         conn,
		dataSource:   dataSrc,
		loOrderKey:   0,
		localCounter: 0,
	}, nil
}

func (c *TxnClient) CreateFreshness(nTxnClients int) error {
	for i := 0; i < nTxnClients; i++ {
		sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS HAT.FRESHNESS%d
(F_TXNNUM INTEGER, F_CLIENTNUM INTEGER)
/*T! SHARD_ROW_ID_BITS=6 */`, i)
		if _, err := c.conn.Exec(context.Background(), sql); err != nil {
			return err
		}
	}
	return nil
}

func (c *TxnClient) Prepare(ctx context.Context) error {
	for _, x := range txnCommands {
		_, err := c.conn.Prepare(ctx, x.Name, x.Cmd)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TxnClient) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}

func iArrayToString(a []int, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}

func fArrayToString(a []float64, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
}

func (c *TxnClient) NewOrderTransactionPS(ctx context.Context, lowestOrderKey int) error {
	// Create a random LO_CUSTNAME
	customerName := c.dataSource.RandCustomerName()

	// Choose a random number of orders
	numOrders := int(c.dataSource.UniformIntDist(1, 7))

	var (
		partKeys       []int     = make([]int, numOrders)
		suppNames      []string  = make([]string, numOrders)
		dateNames      []string  = make([]string, numOrders)
		ordPriorities  []string  = make([]string, numOrders)
		shipPriorities []int     = make([]int, numOrders)
		quantities     []int     = make([]int, numOrders)
		extendedPrices []int     = make([]int, numOrders)
		discounts      []int     = make([]int, numOrders)
		revenues       []float64 = make([]float64, numOrders)
		supplyCosts    []float64 = make([]float64, numOrders)
		taxes          []int     = make([]int, numOrders)
		shipModes      []string  = make([]string, numOrders)
	)
	for i := 0; i < numOrders; i++ {
		// Create a random LO_PARTKEY
		partKeys[i] = c.dataSource.RandPartKeys()
		// random LO_SUPPNAME
		suppNames[i] = c.dataSource.RandSuppKey()
		// random LO_DATENAME
		dateNames[i] = c.dataSource.RandDate()
		// Create the other data of the current lineorder randomly
		ordPriorities[i] = c.dataSource.RandOrdPriority()
		shipPriorities[i] = c.dataSource.RandShipPriorities()
		quantity := c.dataSource.RandQuantity()
		quantities[i] = quantity
		extendedPrices[i] = quantity
		discount, revenue := c.dataSource.RandDiscount()
		discounts[i] = discount
		revenues[i] = revenue
		supplyCosts[i] = c.dataSource.RandSupplyCosts()
		taxes[i] = c.dataSource.RandTaxes()
		shipModes[i] = c.dataSource.RandShipModes()
	}
	tableName := fmt.Sprintf("freshness%d", c.numClients)
	_, err := c.conn.Exec(
		ctx,
		txnQueries[0],
		lowestOrderKey,
		numOrders,
		customerName,
		iArrayToString(partKeys, ","),
		strings.Join(suppNames, ","),
		strings.Join(dateNames, "^"),
		strings.Join(ordPriorities, ","),
		iArrayToString(shipPriorities, ","),
		iArrayToString(quantities, ","),
		iArrayToString(extendedPrices, ","),
		iArrayToString(discounts, ","),
		fArrayToString(revenues, ","),
		fArrayToString(supplyCosts, ","),
		iArrayToString(taxes, ","),
		strings.Join(shipModes, ","),
		tableName,
		c.localCounter)
	if err != nil {
		return fmt.Errorf("%s, orderKey=%d, numOrder=%d", err, lowestOrderKey, numOrders)
	}
	fmt.Printf("done!\n")
	return nil
}
