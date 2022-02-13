package fenixTestDatashared

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"time"
)

// Hash a single value
func HashSingleValue(valueToHash string) (hashValue string) {

	hash := sha256.New()
	hash.Write([]byte(valueToHash))
	hashValue = hex.EncodeToString(hash.Sum(nil))

	return hashValue

}

func HashValues(valuesToHash []string, isNonHashValue bool) string {

	hashString := ""
	sha256Hash := ""

	// Concatenate array position to its content if it is a 'NonHashValue'
	if isNonHashValue == true {
		for valuePosition, value := range valuesToHash {
			valuesToHash[valuePosition] = value + strconv.Itoa(valuePosition)
		}
	}

	// Always sort values before hash them
	sort.Strings(valuesToHash)

	//Hash all values
	for _, valueToHash := range valuesToHash {

		hashString = hashString + valueToHash
		hashString = HashSingleValue(hashString)

	}

	return sha256Hash

}

// Generate DataBaseTimeStamp, eg '2022-02-08 17:35:04.000000'
func GenerateDatetimeTimeStampForDB() (currentTimeStampAsString string) {

	timeStampLayOut := "2006-01-02 15:04:05.000000" //milliseconds
	currentTimeStamp := time.Now()
	currentTimeStampAsString = currentTimeStamp.Format(timeStampLayOut)

	return currentTimeStampAsString
}
