package pg

import (
	"fmt"
	"math"
	"math/rand"
)

type DataSource struct {
	scaleFactor float64
}

func NewDataSource(sf float64) DataSource {
	return DataSource{scaleFactor: sf}
}

func (s *DataSource) CustSize() int64 {
	return int64(math.Floor(30000 * s.scaleFactor))
}

func (s *DataSource) SuppSize() int64 {
	return int64(math.Floor(2000 * s.scaleFactor))
}

func (s *DataSource) PartSize() int64 {
	return 200000 * int64(math.Floor(1+math.Log2(s.scaleFactor)))
}

func (s *DataSource) UniformIntDist(minVal, maxVal int) int64 {
	return int64(minVal) + int64(rand.Intn(maxVal-minVal))
}

func (s *DataSource) UniformRealDist(minVal, maxVal float64) float64 {
	return minVal + rand.Float64()*(maxVal-minVal)
}

var (
	// MonthNames lists names of months, which are used in builtin time function `monthname`.
	monthNames = []string{
		"January", "February",
		"March", "April",
		"May", "June",
		"July", "August",
		"September", "October",
		"November", "December",
	}
	maxDaysInMonth = []int{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	optsYear       = []int{1992, 1993, 1994, 1995, 1996, 1997, 1998}
	partName       = []string{
		"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush",
		"brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower",
		"cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest",
		"frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory",
		"khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium",
		"metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid",
		"pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy",
		"royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring",
		"steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow"}
	//S1(0-5), S2(6-10), S3(11-15)
	partType = []string{
		"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO", "ANODIZED", "BURNISHED",
		"PLATED", "POLISHED", "BRUSHED", "TIN", "NICKEL", "BRASS", "STEEL", "COPPER"}
	partContainer = []string{
		"SM", "LG", "MED", "JUMBO", "WRAP", "CASE", "BOX", "BAG", "JAR",
		"PKG", "PACK", "CAN", "DRUM"}
	ordPriority = []string{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}
	shipMode    = []string{"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"}
	nation      = []string{
		"ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA",
		"FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN",
		"JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA",
		"SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES",
	}
	region = []string{
		"AFRICA", "AMERICA", "AMERICA", "AMERICA", "AFRICA", "AFRICA",
		"EUROPE", "EUROPE", "ASIA", "ASIA", "MIDDLE EAST", "MIDDLE EAST", "ASIA",
		"AFRICA", "AFRICA", "AFRICA", "AFRICA", "AMERICA", "ASIA", "EUROPE",
		"MIDDLE EAST", "ASIA", "EUROPE", "EUROPE", "AMERICA",
	}
	mktsegment = []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"}
)

func (s *DataSource) RandCustName() string {
	custKey := s.UniformIntDist(1, int(s.CustSize()+1))
	return fmt.Sprintf("Customer#%09d", custKey)
}

func (s *DataSource) RandPartKeys() int {
	return int(s.UniformIntDist(1, int(s.PartSize())))
}

func (s *DataSource) RandSuppKey() string {
	suppKey := int(s.UniformIntDist(1, int(s.SuppSize())) + 1)
	return fmt.Sprintf("Supplier#%09d", suppKey)
}

func (s *DataSource) RandDate() string {
	month := s.UniformIntDist(0, len(monthNames))
	// TODO: handle day of Feb
	day := s.UniformIntDist(1, maxDaysInMonth[month]+1)
	year := optsYear[s.UniformIntDist(0, len(optsYear))]
	return fmt.Sprintf("%s %d, %d", monthNames[month], day, year)
}

func (s *DataSource) RandOrdPriority() string {
	o := s.UniformIntDist(0, len(ordPriority))
	return ordPriority[o]
}

func (s *DataSource) RandShipPriorities() int {
	return int(s.UniformIntDist(0, 2))
}

func (s *DataSource) RandQuantity() int {
	return int(s.UniformIntDist(1, 51))
}

func (s *DataSource) RandDiscount() (int, float64) {
	discount := int(s.UniformIntDist(0, 11))
	return discount, float64(100.0-discount) / 100.0
}

func (s *DataSource) RandSupplyCosts() float64 {
	return s.UniformRealDist(1.00, 1000.00)
}

func (s *DataSource) RandTaxes() int {
	return int(s.UniformIntDist(0, 9))
}

func (s *DataSource) RandShipModes() string {
	return shipMode[s.UniformIntDist(0, len(shipMode))]
}
