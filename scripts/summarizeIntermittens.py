#!/usr/bin/python

import sys
import os
import pickle
import subprocess
from os import listdir
from os.path import isfile, join
import time

"""
This script will summary failed tests results and determine if any of them may be intermittents.  For
tests that are determined to be intermittents, a dictionary structure will be generated to store information
about the intermittents.

Currently, a simple threshold test is used to determine if a test is intermittent.  If the failure count of
any test exceed the threshold, we will label it as intermittent.  In particular, the following information
will be stored for each intermittent failure:
        "JenkinsJobName"
        "BuildID"
        "Timestamp"
        "GitHash"
        "TestCategory"
        "NodeName"
        "FailureMessages"
        "FailureCount"

"""

# --------------------------------------------------------------------
# Main program
# --------------------------------------------------------------------

g_test_root_dir = os.path.dirname(os.path.realpath(__file__)) # directory where we are running out code from
g_threshold_failure = 0
g_summary_dict_name = ''
g_AWS_file_path = ''
g_file_start = []

g_summary_dict_intermittents = dict()

def init_intermittents_dict():
    """
    initialize the fields of dictionary storing failed tests.
    :return:
    """
    global g_summary_dict_intermittents
    g_summary_dict_intermittents["TestName"] = []
    g_summary_dict_intermittents["TestInfo"] = []


def usage():
    """
    Print USAGE help.
    """
    print("")
    print("Usage:  ")
    print("python summarizeINtermittents threshold Filename_for_dict AWS_path Failed_PyUnits_summary_dict_from ....")
    print("- threshold is an integer for which a failed test is labeled intermittent if its number of "
          "failure exceeds it.")
    print("- Filename_for_dict is a string denoting the name of the dictionary that will store the final intermittents.")
    print("- AWS_path is a string denoting something like: s3://ai.h2o.tests/jenkins and it is where you store your"
          "data at.")
    print("- Failed_PyUnits_summary_dict_from is a string denoting the beginning of pickle files that contains"
          "")
    print("- ... denotes extra strings that represent the beginning of pickle files that you want us to summarize"
          "for you.")

def copyFilesToLocal():
    """
    This function will go to the AWS path and try to grab the files for us to analyze.  This will fail if run
    on local MAC.

    :return: None
    """
    for ind in range(0, len(g_file_start)):
        full_command = 's3cmd '+g_AWS_file_path+'/'+g_file_start[ind]+'*'

        try:
            subprocess.call(full_command,shell=True)
        except:
            continue

def summarizeFailedRuns():
    """
    This function will look at the local directory and pick out files that have the correct start name and
    summarize the results into one giant dict.

    :return: None
    """
    onlyFiles = [x for x in listdir(g_test_root_dir) if isfile(join(g_test_root_dir, x))]   # grab files

    for f in onlyFiles:
        for fileStart in g_file_start:
            if fileStart in f:  # found the file containing failed tests
                fFullPath = os.path.join(g_test_root_dir, f)
                with open(fFullPath, 'rb') as dataFile:
                    temp_dict = pickle.load(dataFile)   # load in the file with dict containing failed tests

                    # scrape through temp_dict and see if we need to add the test to intermittents
                    extractIntermittents(temp_dict)
                break


def extractIntermittents(temp_dict):
    """
    This function will look through temp_dict and extract the test that failed more than the threshold and add to the
    giant dictionary containing the info

    :param temp_dict:
    :return:
    """
    global g_summary_dict_intermittents
    for ind in range(len(temp_dict)):
        if (temp_dict["TestInfo"][ind]["FailureCount"] >= g_threshold_failure):
            addIntermittent(temp_dict, ind)


def addIntermittent(temp_dict, index):
    global g_summary_dict_intermittents
    testName = temp_dict["TestName"][index]
    testNameList = g_summary_dict_intermittents.keys()
    # check if new intermittents or old ones
    if testName in testNameList:
        testIndex =testNameList.index(testName) # update the test
        updateIntermittentInfo(temp_dict["TestInfo"][index], testIndex, False)
    else:    # new intermittent uncovered
        g_summary_dict_intermittents["TestName"].append(testName)
        updateIntermittentInfo(temp_dict["TestInfo"][index], len(g_summary_dict_intermittents["TestName"]), True)


def updateIntermittentInfo(one_test_info, testIndex, newTest):
    """
    For each test, a dictionary structure will be built to record the various info about that test's failure
    information.  In particular, for each failed tests, there will be a dictionary associated with that test
    stored in the field "TestInfo" of g_faiiled_tests_info_dict.  The following fields are included:
        "JenkinsJobName": job name
        "BuildID"
        "Timestamp": in seconds
        "GitHash"
        "TestCategory": JUnit, PyUnit, RUnit or HadoopPyUnit, HadoopRUnit
        "NodeName": name of machine that the job was run on
        "FailureCount": integer counting number of times this particular test has failed.  An intermittent can be
          determined as any test with FailureCount >= 2.
        "FailureMessages": contains failure messages for the test
    :return: a new dict for that test
    """
    if newTest: # setup the dict structure to store the new data
        g_summary_dict_intermittents["TestInfo"].append(dict())
        g_summary_dict_intermittents["TestInfo"][testIndex]["JenkinsJobName"]=[]
        g_summary_dict_intermittents["TestInfo"][testIndex]["BuildID"]=[]
        g_summary_dict_intermittents["TestInfo"][testIndex]["Timestamp"]=[]
        g_summary_dict_intermittents["TestInfo"][testIndex]["GitHash"]=[]
        g_summary_dict_intermittents["TestInfo"][testIndex]["TestCategory"]=[]
        g_summary_dict_intermittents["TestInfo"][testIndex]["NodeName"]=[]
        g_summary_dict_intermittents["TestInfo"][testIndex]["FailureCount"]=0

    g_summary_dict_intermittents["TestInfo"][testIndex]["JenkinsJobName"].extend(one_test_info["JenkinsJobName"])
    g_summary_dict_intermittents["TestInfo"][testIndex]["BuildID"].extend(one_test_info["BuildID"])
    g_summary_dict_intermittents["TestInfo"][testIndex]["Timestamp"].extend(one_test_info["Timestamp"])
    g_summary_dict_intermittents["TestInfo"][testIndex]["GitHash"].extend(one_test_info["GitHash"])
    g_summary_dict_intermittents["TestInfo"][testIndex]["TestCategory"].extend(one_test_info["TestCategory"])
    g_summary_dict_intermittents["TestInfo"][testIndex]["NodeName"].extend(one_test_info["NodeName"])
    g_summary_dict_intermittents["TestInfo"][testIndex]["FailureCount"] += one_test_info["NodeName"]


def printSaveIntermittens():
    """
    This function will print out the intermittents onto the screen for casual viewing.  It will also print out
    where the giant summary dictionary is going to be stored.

    :return: None
    """
    for ind in len(g_summary_dict_intermittents):
        testName = g_summary_dict_intermittents["TestName"][ind]
        numberFailure = g_summary_dict_intermittents["TestInfo"][ind]["FailureCount"]
        firstFailedTS = min(g_summary_dict_intermittents["TestInfo"][ind]["Timestamp"])

        print("Intermittent test: {0} has failed {1} times in the past since {2}".format(testName, numberFailure,
                                                                                         time.ctime(firstFailedTS)))
    # save dict in file
    with open(g_summary_dict_name, 'wb') as writeFile:
        pickle.dump(g_summary_dict_intermittents, writeFile)


def main(argv):
    """
    Main program.  Expect script name plus  inputs in the following order:
    - This script name
    1. threshold: integer that will denote when a failed test will be declared an intermittent
    2. string that denote the beginning of a file containing failed tests info.
    3. Optional strings that denote the beginning of a file containing failed tests info.

    @return: none
    """
    global g_script_name
    global g_test_root_dir
    global g_threshold_failure
    global g_AWS_file_path
    global g_file_start
    global g_summary_dict_name

    if len(argv) < 6:
        print "Wrong call.  Not enough arguments.\n"
        usage()
        sys.exit(1)
    else:   # we may be in business
        g_threshold_failure = int(argv[1])
        g_summary_dict_name = os.path.join(g_test_root_dir, argv[2])
        g_AWS_file_path = argv[3]

        for ind in range(4, len(argv)):
            g_file_start.append(argv[ind])

        copyFilesToLocal()
        init_intermittents_dict()
        summarizeFailedRuns()
        printSaveIntermittens()


if __name__ == "__main__":
    main(sys.argv)
