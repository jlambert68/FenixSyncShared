package fenixTestDatashared

import (
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"log"
	"sort"
	"strings"
)

func setFromList(list []string) (set []string) {
	ks := make(map[string]bool) // map to keep track of repeats

	for _, e := range list {
		if _, v := ks[e]; !v {
			ks[e] = true
			set = append(set, e)
		}
	}
	return
}

func uniqueGotaSeries(s series.Series) series.Series {
	return series.New(setFromList(s.Records()), s.Type(), s.Name)
}

func uniqueGotaSeriesAsStringArray(s series.Series) []string {
	return uniqueGotaSeries(s).Records()
}

func hashChildrenAndWriteToDataStore(level int, currentMerklePath string, valuesToHash []string, isEndLeafNode bool) string {

	hashString := ""
	sha256Hash := ""

	// Sort Array before hashing
	sort.Strings(valuesToHash)

	// Hash all leaves for rowHashValue in valuesToHash
	for _, valueToHash := range valuesToHash {

		hashString = sha256Hash + valueToHash
		sha256Hash = HashSingleValue(hashString)

	}

	MerkleHash := sha256Hash

	// # Add MerkleHash and sub leaf nodes to table if not end node. If End node then only save ref to itself
	if isEndLeafNode == true {
		// Add row
		//merkleTreeToUse.loc[merkleTreeToUse.shape[0]] = [level, currentMerklePath, MerkleHash, MerkleHash]
		newRowDataFrame := dataframe.New(
			series.New([]int{level}, series.Int, "MerkleLevel"),
			series.New([]string{currentMerklePath}, series.String, "MerklePath"),
			series.New([]string{MerkleHash}, series.String, "MerkleHash"),
			series.New([]string{MerkleHash}, series.String, "MerkleChildHash"),
		)

		tempDF := merkleTreeDataFrame.RBind(newRowDataFrame)
		merkleTreeDataFrame = tempDF

	} else {
		for _, rowHashValue := range valuesToHash {
			// Add row
			//merkleTreeToUse.loc[merkleTreeToUse.shape[0]] = [level, currentMerklePath, MerkleHash, rowHashValue ]
			newRowDataFrame := dataframe.New(
				series.New([]int{level}, series.Int, "MerkleLevel"),
				series.New([]string{currentMerklePath}, series.String, "MerklePath"),
				series.New([]string{MerkleHash}, series.String, "MerkleHash"),
				series.New([]string{rowHashValue}, series.String, "MerkleChildHash"),
			)
			tempDF := merkleTreeDataFrame.RBind(newRowDataFrame)
			merkleTreeDataFrame = tempDF

		}
	}

	return MerkleHash

}

func recursiveTreeCreator(level int, currentMerkleFilterPath string, dataFrameToWorkOn dataframe.DataFrame, currentMerklePath string, dataFrameToAddLeafNodeHashTo *dataframe.DataFrame) string {
	level = level + 1
	startPosition := 0
	endPosition := strings.Index(currentMerkleFilterPath, "/")

	// Check if we found end of Tree
	if endPosition == -1 {
		// Leaf node, process rows

		// Sort on row - hashes
		dataFrameToWorkOn = dataFrameToWorkOn.Arrange(dataframe.Sort("TestDataHash"))

		// Hash all row - hashes into one hash
		valuesToHash := uniqueGotaSeriesAsStringArray(dataFrameToWorkOn.Col("TestDataHash"))

		// Hash and store
		MerkleHash := hashChildrenAndWriteToDataStore(level, currentMerklePath, valuesToHash, true)

		// Loop over rows and set LeafNodeHash
		numberOfRows := dataFrameToWorkOn.Nrow()
		testDataLeafNodeHashColumn := dataFrameToWorkOn.Ncol() - 1

		for rowCounter := 0; rowCounter < numberOfRows; rowCounter++ {
			dataFrameToWorkOn.Elem(rowCounter, testDataLeafNodeHashColumn).Set(MerkleHash)

		}

		// Concatenate testdatarows to Orginal TestData dataframe
		headerKeys := dataFrameToWorkOn.Names()
		*dataFrameToAddLeafNodeHashTo = dataFrameToAddLeafNodeHashTo.OuterJoin(dataFrameToWorkOn, headerKeys...)

		return MerkleHash

	} else {
		// Get merklePathLabel
		merklePathLabel := currentMerkleFilterPath[startPosition:endPosition]
		currentMerkleFilterPath := currentMerkleFilterPath[endPosition+1:]

		// Get Unique values
		uniqueValuesForSpecifiedColumn := uniqueGotaSeriesAsStringArray(dataFrameToWorkOn.Col(merklePathLabel))

		var valuesToHash []string

		// Loop over all unique values in column
		for _, uniqueValue := range uniqueValuesForSpecifiedColumn {
			//newFilteredDataFrame := dataFrameToWorkOn[dataFrameToWorkOn[merklePathLabel] == uniqueValue]
			newFilteredDataFrame := dataFrameToWorkOn.Filter(
				dataframe.F{
					Colname:    merklePathLabel,
					Comparator: series.Eq,
					Comparando: uniqueValue,
				})

			// Recursive call to get next level, if there is one
			localMerkleHash := recursiveTreeCreator(level, currentMerkleFilterPath, newFilteredDataFrame, currentMerklePath+uniqueValue+"/", dataFrameToAddLeafNodeHashTo)

			if len(localMerkleHash) != 0 {
				valuesToHash = append(valuesToHash, localMerkleHash)
			} else {
				log.Fatalln("We are at the end node - **** Should never happened ****")
			}
		}

		// Add MerkleHash and nodes to table
		merkleHash := hashChildrenAndWriteToDataStore(level, currentMerklePath, valuesToHash, false)
		return merkleHash

	}
	return ""
}

// Dataframe holding original File's MerkleTree
var merkleTreeDataFrame dataframe.DataFrame

// Dataframe holding changed File's MerkleTree
//var changedFilesMerkleTreeDataFrame dataframe.DataFrame

/*
NOT USED

// Process incoming csv file and create MerkleRootHash and MerkleTree
func LoadAndProcessFile(fileToprocess string) (string, dataframe.DataFrame, dataframe.DataFrame) {

	irisCsv, err := os.Open(fileToprocess)
	if err != nil {
		log.Fatal(err)
	}
	defer irisCsv.Close()

	df := dataframe.ReadCSV(irisCsv,
		dataframe.WithDelimiter(';'),
		dataframe.HasHeader(true))

	merkleHash, merkleTreeDataFrame, _ := CreateMerkleTreeFromDataFrame(df, "not used")

	return merkleHash, merkleTreeDataFrame, df
}

*/

// Create MerkleRootHash and MerkleTree
func CreateMerkleTreeFromDataFrame(df dataframe.DataFrame, merkleFilterPath string) (merkleHash string, merkleTreeDataFrame dataframe.DataFrame, testDataRowsWithLeafNodeHashAdded dataframe.DataFrame) {
	df = df.Arrange(dataframe.Sort("TestDataId"))

	numberOfRows := df.Nrow()

	// Add column to hold RowHash
	df = df.Mutate(
		series.New(make([]string, numberOfRows), series.String, "TestDataHash"))

	// Add column to hold LeafNodeHash
	df = df.Mutate(
		series.New(make([]string, numberOfRows), series.String, "LeafNodeHash"))

	// Don't process 'TestDataHash' and 'LeafNodeHash'
	numberOfColumnsToProcess := df.Ncol() - 2
	testDataHashColumn := numberOfColumnsToProcess

	// Loop columns and add RowHash for each row
	for rowCounter := 0; rowCounter < numberOfRows; rowCounter++ {
		var valuesToHash []string
		for columnCounter := 0; columnCounter < numberOfColumnsToProcess; columnCounter++ {
			valueToHash := df.Elem(rowCounter, columnCounter).String()
			valuesToHash = append(valuesToHash, valueToHash)
		}

		// Hash all values for row
		hashedRow := HashValues(valuesToHash, true)
		df.Elem(rowCounter, testDataHashColumn).Set(hashedRow)

	}

	// Columns for MerkleTree DataFrame
	merkleTreeDataFrame = dataframe.New(
		series.New([]int{}, series.Int, "MerkleLevel"),
		series.New([]string{}, series.String, "MerklePath"),
		series.New([]string{}, series.String, "MerkleHash"),
		series.New([]string{}, series.String, "MerkleChildHash"),
	)

	//merkleFilterPath :=  //"AccountEnvironment/ClientJuristictionCountryCode/MarketSubType/MarketName/" //SecurityType/"

	testDataRowsWithLeafNodeHashAdded = df.Copy()
	testDataRowsWithLeafNodeHashAdded = testDataRowsWithLeafNodeHashAdded.Filter(
		dataframe.F{
			Colname:    "LeafNodeHash",
			Comparator: series.Eq,
			Comparando: "There is no spoon",
		})

	merkleHash = recursiveTreeCreator(0, merkleFilterPath, df.Copy(), "MerkleRoot/", &testDataRowsWithLeafNodeHashAdded)

	return merkleHash, merkleTreeDataFrame, testDataRowsWithLeafNodeHashAdded
}

// Calculate MerkleHash from leaf nodes in MerkleTree
func calculateMerkleHashFromMerkleTreeLeafNodes(merkleLevel int, merkleTreeLeafNodes dataframe.DataFrame, maxMerkleLevel int) (merkleHash string) {

	merkleLevel = merkleLevel + 1

	// If we are at a single leaf node then return its Hash value
	if merkleLevel > maxMerkleLevel { // merkleTreeLeafNodes.Nrow() == 1 {
		merkleHash = uniqueGotaSeriesAsStringArray(merkleTreeLeafNodes.Col("MerkleHash"))[0]

		return merkleHash
	}

	// Extract current node in merklePathLabel and store
	merkleTreeLeafNodes = merkleTreeLeafNodes.Arrange(dataframe.Sort("MerklePath"))

	numberOfRows := merkleTreeLeafNodes.Nrow()
	merklePathNodeColumn := merkleTreeLeafNodes.Ncol() - 1 // CurrentMerklePathNode
	merklePathColumn := 1

	for rowCounter := 0; rowCounter < numberOfRows; rowCounter++ {
		merklePath := merkleTreeLeafNodes.Elem(rowCounter, merklePathColumn).String()

		startPosition := 0
		endPosition := strings.Index(merklePath, "/")
		merklePathLabel := merklePath[startPosition:endPosition]

		// Store the extracted merklePathLabel
		merkleTreeLeafNodes.Elem(rowCounter, merklePathNodeColumn).Set(merklePathLabel)

		// Create new MerklePath for next node level
		merklePath = merklePath[endPosition+1:]

		// Store new MerklePath as next node level
		merkleTreeLeafNodes.Elem(rowCounter, merklePathColumn).Set(merklePath)
	}

	// Get Unique values for merklePathLabel
	uniqueValuesForSpecifiedColumn := uniqueGotaSeriesAsStringArray(merkleTreeLeafNodes.Col("CurrentMerklePathNode"))

	var valuesToHash []string

	var localMerkleHash string
	// Loop over all unique values in column 'CurrentMerklePathNode'
	for _, uniqueValue := range uniqueValuesForSpecifiedColumn {
		newFilteredDataFrame := merkleTreeLeafNodes.Filter(
			dataframe.F{
				Colname:    "CurrentMerklePathNode",
				Comparator: series.Eq,
				Comparando: uniqueValue,
			})

		// Recursive call to get next level, if there is one
		if newFilteredDataFrame.Nrow() > 0 {
			localMerkleHash = calculateMerkleHashFromMerkleTreeLeafNodes(merkleLevel, newFilteredDataFrame, maxMerkleLevel)
		} else {

			merkleHash = uniqueGotaSeriesAsStringArray(merkleTreeLeafNodes.Col("MerkleHash"))[0]

			return merkleHash
		}

		// Check if we come all the way up to MerkleRoot again. Then return current MerkleRootHash
		if uniqueValue == "MerkleRoot" {
			return localMerkleHash
		}

		// Append returned hash to list of hashes
		if len(localMerkleHash) != 0 {
			valuesToHash = append(valuesToHash, localMerkleHash)
		} else {
			log.Fatalln("We are at the end node - **** Should never happened ****")
		}
	}

	// Hash the hashes into parent nodes hash value
	merkleHash = HashValues(valuesToHash, false)

	return merkleHash

}

// CalculateMerkleHashFromMerkleTree Calculate MerkleHash from leaf nodes in MerkleTree
func CalculateMerkleHashFromMerkleTree(merkleTree dataframe.DataFrame) (merkleHash string) {

	// Filter out the leaf nodes
	leaveNodeLevel := merkleTree.Col("MerkleLevel").Max()
	merkleTreeLeafNodes := merkleTree.Filter(
		dataframe.F{
			Colname:    "MerkleLevel",
			Comparator: series.Eq,
			Comparando: int(leaveNodeLevel)})

	// Add column for storing current node path
	numberOfRows := merkleTreeLeafNodes.Nrow()

	// If there are no leaf nodes then it's a problem
	if numberOfRows == 0 {
		return "-1"
	}

	merkleTreeLeafNodes = merkleTreeLeafNodes.Mutate(
		series.New(make([]string, numberOfRows), series.String, "CurrentMerklePathNode"))

	merkleHash = calculateMerkleHashFromMerkleTreeLeafNodes(0, merkleTreeLeafNodes, int(leaveNodeLevel))

	return merkleHash
}

//TODO add logging and error handling for each function...

// ExtractMerkleRootHashFromMerkleTree Retrieve MerkleRootHashFromMerkleTree
func ExtractMerkleRootHashFromMerkleTree(merkleTree dataframe.DataFrame) (merkleRootHash string) {

	// Filter out the MerkleRoot node
	leaveNodeLevel := merkleTree.Col("MerkleLevel").Min()
	merkleTreeRoot := merkleTree.Filter(
		dataframe.F{
			Colname:    "MerkleLevel",
			Comparator: series.Eq,
			Comparando: int(leaveNodeLevel)})

	// Extract MerkleRootHash
	merkleRootHashArray := uniqueGotaSeriesAsStringArray(merkleTreeRoot.Col("MerkleHash"))

	// The result should be just one line
	if len(merkleRootHashArray) != 1 {
		log.Fatalln(" The result should be just one line for MerkleRootHash. Ending this misery! ")
		merkleRootHash = "666"

	} else {
		merkleRootHash = merkleRootHashArray[0]
	}

	return merkleRootHash
}

func MissedPathsToRetreiveFromClient(serverCopyMerkleTree dataframe.DataFrame, newClientMerkleTree dataframe.DataFrame) (merklePathsToRetreive []string) {

	merkleDataToKeep := serverCopyMerkleTree.InnerJoin(newClientMerkleTree, "MerkleLevel", "MerklePath", "MerklePath", "MerkleHash", "MerkleChildHash")

	//Filter out rows that is missing and is not on 'highest' MerkleLevel (leaves)
	leaveNodeLevel := merkleDataToKeep.Col("MerkleLevel").Max()
	isNotInListFkn := IsNotInListFilter(merkleDataToKeep.Col("MerkleChildHash").Records())

	merkleTreeToRetrieveTemp := newClientMerkleTree.Filter(
		dataframe.F{
			Colname:    "MerkleLevel",
			Comparator: series.Eq,
			Comparando: leaveNodeLevel})

	merkleTreeToRetrieve := merkleTreeToRetrieveTemp.Filter(
		dataframe.F{
			Colname:    "MerkleChildHash",
			Comparator: series.CompFunc,
			Comparando: isNotInListFkn()})

	merklePathsToRetreive = merkleTreeToRetrieve.Col("MerklePath").Records()

	//Clean up MerklePaths to be sent
	for arrayPosition, merklePath := range merklePathsToRetreive {
		numberOfInstances := strings.Count(merklePath, "MerkleRoot/")
		if numberOfInstances != 1 {
			log.Println("'MerkleRoot/' was not found: ", merklePath)
		}
		cleanedValue := merklePath[11:]
		merklePathsToRetreive[arrayPosition] = cleanedValue
	}
	return merklePathsToRetreive

}

func IsNotInListFilter(arrayToCompareWith []string) func() func(el series.Element) bool {
	isNaNFunction := func() func(el series.Element) bool {
		return func(el series.Element) bool {
			var notFoundInArray = true

			for _, value := range arrayToCompareWith {
				if value == el.String() {
					notFoundInArray = false
					break
				}
			}

			return notFoundInArray
		}
	}
	return isNaNFunction
}
