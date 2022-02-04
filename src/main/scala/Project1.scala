import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.util.Scanner
import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.io.PrintWriter
import java.io.File
import java.io.FileInputStream
object Project1 {
    System.setSecurityManager(null)
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    private var scanner = new Scanner(System.in)
    private var statement: Statement = null
    private val log = new PrintWriter(new File("query.log"))
    private var resultSet: ResultSet = null
    private var usersTable: ResultSet = null
    private var individualRecord: ResultSet = null
    private var loggedInUser: ResultSet = null
    private var loggedInUserID: Int = 0
    private var loggedInUsername: String = "null"
    private var loggedInPassword: String = "null"
    private var loggedInAccountType: String = "null"
    private val conf = new SparkConf().setMaster("local").setAppName("Project1")
    private val sc = new SparkContext(conf)
    private val hiveCtx = new HiveContext(sc)
    def main(args: Array[String]): Unit = {
        sc.setLogLevel("ERROR")
        import hiveCtx.implicits._
        val driver = "com.mysql.cj.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/project1"
        val username = "root"
        val password = "Reva2021!@my"
        scanner.useDelimiter(System.lineSeparator())
        Class.forName(driver)
        val connection = DriverManager.getConnection(url, username, password)
        statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        mainMenu()
        sc.stop()
        connection.close()
        println("Thank you for visiting us!")
        println("++++++++++++++++++++++++++++++++++++++")
    }
     def mainMenu(): Unit = {
        var continue = true
        while(continue) {
            var selection = getInput()
            if (selection == "1") {
                createAccount()
            }
            if (selection == "2") {
                login()
                var accountType = loggedInUser.getString("account_type")
                if (accountType == "user") userButton()
                else adminButton()
            }
            if (selection == "3") {
                continue = false
            }
        }
    }
def getInput(): String = {
        println("")
        println("WELCOME TO FACEBOOK DATA!")
        println("PLEASE SELECT THE BELOW OPTION.")
        println("++++++++++++++++++++++++++++++++++++++++")
        println("1: TO CREATE ACCOUNT")
        println("2: TO LOGIN")
        println("3: Exit")
        println("+++++++++++++++++++++++++++++++++++++++")
        return scanner.nextLine().trim()
    }
    def createAccount(): Unit = {
        var resultSet = statement.executeQuery("SELECT username FROM users;")
        log.write("Executing 'SELECT username FROM users;'")
        println("++++++++++++++++++++++++++++++++++++++")
        println("TO CREATE YOUR ACCOUNT.")
        println("please create your username.")
        println("")
        var newUsername = ""
        var newPassword = ""
        var accountType = ""
        var inputUserName = true
        while (inputUserName) {
            inputUserName = false
            newUsername = scanner.nextLine().trim()
            resultSet.beforeFirst()
            while (resultSet.next()) {
                var rsUserName = resultSet.getString("username")
                if (newUsername == rsUserName) {
                    println("This username already exists. Please try with new username.")
                    inputUserName = true
                }
            }
        }
        println("Now, please create a password.")
        var inputPassword = true
        while (inputPassword) {
            inputPassword = false
            newPassword = scanner.nextLine().trim()
            if (newPassword.length < 3) {
                println("Please enter 3 characters to meet the password standard.")
                inputPassword = true
            }
        }
        println("Please press 1 for USER account or press for 2 an ADMIN account.")
        println("1: USER")
        println("2: ADMIN")
        var inputAccountType = true
        while (inputAccountType) {
            inputAccountType = false
            var selection = scanner.nextLine().trim()
            if (selection == "1") {
                accountType = "user"
            }
            else if (selection == "2") {
                accountType = "admin"
            }
            else {
                println("Invalid input please try again.")
                inputAccountType = true
            }
        }
        statement.executeUpdate(s"INSERT INTO users (username, password, account_type) VALUES ('"+newUsername+"', '"+newPassword+"', '"+accountType+"');")
        println("Successfully done!")
        println("+++++++++++++++++++++++++++++++++++++++++++")
    }
def toLoggedInUser(username: String): Unit = {
        loggedInUser = statement.executeQuery(s"SELECT * FROM users WHERE username = '"+username+"';")
        while (loggedInUser.next()) {
            loggedInUserID = loggedInUser.getInt("user_id")
            loggedInUsername = loggedInUser.getString("username")
            loggedInPassword = loggedInUser.getString("password")
            loggedInAccountType = loggedInUser.getString("account_type")
        }
        loggedInUser.beforeFirst()
    }
    def login(): Unit = {    
        var loginButton = true
        while (loginButton) {
            loginButton = false
            println("")
            print("Enter your Username: ")
            var loginUsername = scanner.nextLine().trim()
            toLoggedInUser(loginUsername)
            if (!loggedInUser.next()) {
                loginButton = true
                println("Invalid input please try again.")
            }
            else {
                var wrongPassword = true
                while (wrongPassword) {
                    wrongPassword = false
                    print("Enter your Password: ")
                    var loginPassword = scanner.nextLine().trim()
                    if (loginPassword != loggedInPassword) {
                        wrongPassword = true
                        println("Invalid input please try again.")
                    }
                }
            }
        }
    }
    def userButton(): Unit = {
        var getInUser = true
        while (getInUser) {
            println("WELCOME! THIS FACEBOOK DATA ANALYSIS.")
            println("Please press 1 to access fb data or 2 for sign out.")
            println("1: TO FACEBOOK DATA")
            println("2: TO SIGN OUT")
            println("++++++++++++++++++++++++++++++++++++++++++++++++")
            var userInput = scanner.nextLine().trim()
            if (userInput == "1") {
                facebookData()
            }
            else if (userInput == "2") {
                getInUser = false
                println("")
                println("Thanks")
            }
            else {
                println("Invalid input. Please try again.")
            }
        }
    }
    def adminButton(): Unit = {
        var getInAdmin = true
        while (getInAdmin) {
            println("")
            println("HERE FACEBOOK DATA ANALYSIS IS BEGIN.")
            println("++++++++++++++++++++++++++++++++++++++++")
            println("PLEASE SELECT THE BELOW OPTION.")
            println("1: TO ACCESS USERTABLE.")
            println("2: TO ACCESS FACEBOOK DATA.")
            println("3: SIGN OUT")
            println("")
            var adminInput = scanner.nextLine().trim()
            if (adminInput == "1") {
                getInAdmin = false
                accessUsersTbl()
            }
            else if (adminInput == "2") {
                getInAdmin = false
                facebookData()
            }
            else if (adminInput == "3") {
                getInAdmin = false
                println("")
                println("Thanks!")
            }
            else {
                println("")
                println("Invalid input. Please try again.")
            }
        }
    }
    def getUsersTable(): Unit = {
        usersTable = statement.executeQuery(s"SELECT * FROM users")
    }
    def accessUsersTbl(): Unit = {
        var getInUsertbl = true
        while (getInUsertbl) {
            println("")
            getUsersTable()
            while (usersTable.next()) {
                var id = usersTable.getInt("user_id")
                var username = usersTable.getString("username")
                var password = usersTable.getString("password")
                var account_type = usersTable.getString("account_type")
                println(s"ID=> $id : Username=> $username : Password=> $password : Account Type=> $account_type")
            }
            println("++++++++++++++++++++++++++++++++++++++++")
            println("Please choose one of the options below.")
            println("1: TO edit user information")
            println("2: TO remove user from database.")
            println("3: TO previous page")
            println("+++++++++++++++++++++++++++++++++++++++")
            var selection = scanner.nextLine().trim()
            if (selection == "1") {
                getInUsertbl = false
                editUserInfo()
            }
            else if (selection == "2") {
                removeUser()
            }
            else if (selection == "3") {
                getInUsertbl = false
                if (loggedInAccountType == "user") userButton()
                else adminButton()
            }
        }
    }
def checkIndividualRecord(id: Int): Unit = {
        individualRecord = statement.executeQuery(s"SELECT * FROM users WHERE user_id = $id;")
    }
    def editUserInfo(): Unit = {
        var getInUserInfo = true
        while (getInUserInfo) {
            println("")
            println("Please enter ID to edit a user information.")
            var whichUser = scanner.nextLine().trim()
            checkIndividualRecord(whichUser.toInt)
            if (!individualRecord.next()) {
                println("This user is not exist. Please try again.")
            }
            else {
                var getWhichUser = true
                while (getWhichUser) {
                    println("TO EDIT THE INFORMATION PLEASE SELECT THE BELOW OPTION.")
                    println("1: Username")
                    println("2: Password")
                    println("3: Account Type")
                    println("4: previous page")
                    println("++++++++++++++++++++++++++++++++++++++++++++++++")
                    var whichInfo = scanner.nextLine().trim()
                    if (whichInfo == "1") {
                        println("")
                        println("Please enter a new username.")
                        println("")
                        var newUsername = scanner.nextLine().trim()
                        individualRecord.updateString("username", newUsername)
                        individualRecord.updateRow()
                        println("The new username has been saved.")
                    }
                    else if (whichInfo == "2") {
                        println("")
                        println("Please enter a new password.")
                        println("")
                        var newPassword = scanner.nextLine().trim()
                        individualRecord.updateString("password", newPassword)
                        individualRecord.updateRow()
                        println("The new password has been saved.")
                    }
                    else if (whichInfo == "3") {
                        var getWhichType = true
                        while (getWhichType) {
                            println("")
                            println("Please press 1 for USER and 2 for ADMIN.")
                            println("1: USER")
                            println("2: ADMIN")
                            println("")
                            var newType = scanner.nextLine().trim()
                            if (newType == "1" || newType == "2") {
                                var userType = if (newType == "1") "user" else "admin"
                                getWhichType = false
                                individualRecord.updateString("account_type", userType)
                                individualRecord.updateRow()
                                println("successfully!")
                                println("++++++++++++++++++++++++++++++++++++++++++++")
                            }
                            else {
                                println("Invalid input please try again.")
                            }
                        }
                    }
                    else if (whichInfo == "4") {
                        getInUserInfo = false
                        getWhichUser = false
                        accessUsersTbl()
                    }
                    else {
                        println("Invalid input please try again.")
                    }
                }
            }
        }
    }
    def removeUser(): Unit = {
        var continueRemoveUser = true
        while (continueRemoveUser) {
            println("")
            println("Please enter ID to delete a USER, or type \"back\" to go previous page.")
            var whichUser = scanner.nextLine().trim()
            if (whichUser == "back") {
                continueRemoveUser = false
            }
            else {
                checkIndividualRecord(whichUser.toInt)
                if (!individualRecord.next()) {
                    println("This user is not exist. Please try again.")
                }
                else {
                    individualRecord.deleteRow()
                    continueRemoveUser = false
                    println("")
                    println("This user has been deleted.")
                }
            }
        }
    }
    def facebookData(): Unit = {
        insertFacebookData(hiveCtx)
        var getInFbData = true
        while (getInFbData) {
            println("")
            println("Please choose one of the options below.")
            println("1: Facebook users with minimum and maximum age.")
            println("2: Facebook users who greater than 100 years.")
            println("3: Facebook users who received more than 2500 likes.")
            println("4: Counting facebook users by gender.")
            println("5: Facebook users who big followers.")
            println("6: The total people of using fb and the total likes they make on fb.")
            println("0: previous page")
            println("++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            var userInput = scanner.next().toString()
            if (userInput == "1") {
                minMaxAge()
            }
            else if (userInput == "2") {
                ageGreaterThanH()
            }
            else if (userInput == "3") {
                maxLikes()
            }
            else if (userInput == "4") {
                countByGender()
            }
            else if (userInput == "5") {
                hadMaxFollowers()
            }
            else if (userInput == "6") {
                totalUsers()
            }
            else if (userInput == "0") {
                getInFbData = false
                adminButton()
            }
            else {
                println("Invalid input please try again.")
            }
        }
    }
    def insertFacebookData(hiveCtx: HiveContext): Unit = {
        val output = hiveCtx.read
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("input/pseudo_facebook.csv")
        output.limit(10).show()
        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
        hiveCtx.sql("SET hive.enforce.bucketing=false")
        hiveCtx.sql("SET hive.enforce.sorting=false")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS data1 (userid INT, age INT, dob_day INT, dob_year INT, dob_month INT, gender STRING, tenure_day INT, friend_count INT, friendship_initiated INT, likes INT, likes_received INT)")
        hiveCtx.sql("INSERT INTO data1 SELECT * FROM temp_data")
        // hiveCtx.sql("CREATE TABLE IF NOT EXISTS data1_partitioned(age INT, dob_day INT, dob_year INT, dob_month INT, tenure_day INT, friend_count INT, friendship_initiated INT, likes INT, likes_received INT) PARTITIONED BY(userid INT) CLUSTERED BY (dob_year) INTO 10 BUCKETS row format delimited fields terminated by ',' stored as textfile ")
        // hiveCtx.sql("INSERT INTO data1_partitioned SELECT userid, age, dob_day, dob_year, dob_month, tenure_day, friend_count, friendship_initiated, likes, likes_received, gender FROM data1")
        //val summary = hiveCtx.sql("SELECT * FROM data1_partitioned LIMIT 10")
        val summary = hiveCtx.sql("select * from data1 LIMIT 10")
        summary.show()
    }
    def minMaxAge(): Unit = {
      println("1.Print minimum and maximum age using a facebook.")
        val result = hiveCtx.sql("SELECT gender, MIN(age) AS SmallestAge, MAX(age) AS OldestAge from data1 GROUP BY gender LIMIT 10 ");
        result.show()
        result.write.csv("results/minMaxAge")
    }
    def ageGreaterThanH(): Unit = {
        val result = hiveCtx.sql("select userid, age, dob_year, gender from data1 where age between 100 and 115 order by dob_year DESC LIMIT 20")
        result.show()
        result.write.csv("results/ageGreaterThanH")
    }
    def maxLikes(): Unit = {
        val result = hiveCtx.sql("select * from data1 order by tenure_day DESC ")
        result.show()
        result.write.csv("results/maxLikes")
    }
    def countByGender(): Unit = {
        val result = hiveCtx.sql("select gender, count(*) from data1 group by gender")
        result.show()
        result.write.csv("results/countByGender")
    }
    def hadMaxFollowers(): Unit = {
        val result = hiveCtx.sql("SELECT tenure_day, count(gender) from data1 group by tenure_day")
        result.show()
        result.write.csv("results/hadMaxFollowers")
    }
    def totalUsers(): Unit = {
    //    val result = hiveCtx.sql("SELECT MIN(tenure_day) AS short_term_User, MAX(tenure_day) AS long_term_User from data1 where gender = 'female'")
        val result = hiveCtx.sql("SELECT gender, count(gender) as Total_Users, SUM (likes) as Total_Likes from data1 group by gender LIMIT 10")
        result.show()
        result.write.csv("results/totalUsers")
    }
}
