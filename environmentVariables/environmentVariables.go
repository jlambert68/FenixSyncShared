package environmentVariables

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

// The name of the injected variable that decides if 'injected Environment variables'
// should be used instead in 'pure Environment Variables'
const useInjectedEnvironmentVariablesVariableName = "useInjectedEnvironmentVariables"

// A pointer that references the InjectVariablesMap that normally exists in 'main' in the program
var injectedVariablesMapPtr *map[string]*string

// InitiateInjectedVariablesMap
// Initiate the reference to the 'InjectedVariablesMap'
func InitiateInjectedVariablesMap(tempInjectedVariablesMap *map[string]*string) {
	injectedVariablesMapPtr = tempInjectedVariablesMap
}

// ExtractEnvironmentVariableOrInjectedEnvironmentVariable
// Extracts an environment variables from 'environment variables' or
// from injected variables that was injected during build time or when program is starting.
func ExtractEnvironmentVariableOrInjectedEnvironmentVariable(
	environmentVariableName string) (
	environmentVariableValue string) {

	var err error

	// Check that pointer to 'injectedVariablesMap' is initiated
	if injectedVariablesMapPtr == nil {
		log.Fatalln("'injectedVariablesMapPtr' is not initiated using 'func ExtractEnvironmentVariableOrInjectedEnvironmentVariable(tempInjectedVariablesMap *map[string]*string)' in 'FenixSyncShared/environmentVariables")
	}

	// The Map with injected Environment variables retrieved from pointer
	var tempInjectedVariablesMap map[string]*string
	tempInjectedVariablesMap = *injectedVariablesMapPtr

	// Create the variable name for injected variable
	var injectedVariableNameAsString string
	injectedVariableNameAsString = "Injected_" + environmentVariableName

	// Verify that Variable exists in map for injected Environment Variables
	var injectedVariableValue *string
	var existInMap bool
	injectedVariableValue, existInMap = tempInjectedVariablesMap[injectedVariableNameAsString]
	if existInMap == false {
		// If the 'Injected Variable' is missing then end this misery programs life
		log.Fatalln("Injected Environment variable '" + injectedVariableNameAsString + "' doesn't exist in 'injectedVariablesMap'")
	}

	// Decide if 'Environment Variable' or 'Injected Environment Variable' should be used
	var tempUseInjectedEnvironmentVariablesValueAsString *string
	var tempUseInjectedEnvironmentVariablesValue bool
	tempUseInjectedEnvironmentVariablesValueAsString, existInMap = tempInjectedVariablesMap[useInjectedEnvironmentVariablesVariableName]
	if existInMap == false {
		// If the 'Injected Variable' is missing then end this misery programs life
		log.Fatalln("Injected Environment variable '" + useInjectedEnvironmentVariablesVariableName + "' doesn't exist in 'injectedVariablesMap'")
	} else {
		// Validate that variables only contains a boolean
		tempUseInjectedEnvironmentVariablesValue, err = strconv.ParseBool(*tempUseInjectedEnvironmentVariablesValueAsString)
		if err != nil {
			fmt.Println("Couldn't convert injected environment variable '"+useInjectedEnvironmentVariablesVariableName+"' to a boolean, error: ", *tempUseInjectedEnvironmentVariablesValueAsString, err)
			os.Exit(0)
		}
	}

	// Extract 'Environment Variable' or 'Injected Environment Variable'
	if tempUseInjectedEnvironmentVariablesValue == true {

		// Use Injected Environment Variables
		environmentVariableName = *injectedVariableValue

	} else {

		// Use normal Environment Variables
		environmentVariableValue = os.Getenv(environmentVariableName)

	}

	// // No environment variable found or there is no value. No value is not allowed
	if environmentVariableValue == "" {
		log.Fatalf("Warning: '%s' environment variable is not set.", environmentVariableValue)
	}

	return environmentVariableValue
}
