<?php

/*

     To make the tv screens work, only these tables need to be refreshed hourly
          -> SaleTransactionLineByEmployee [php compiled]
          -> SaleTransactionLine [from this job]
          -> AppointmentAll [from this job]
          -> TransactionsByDateEmployee [MySQL view that draws from SaleTransactionLine]

*/

// Days to look back
$dd = 5;

//MySQL Connection Variables
$servername = "206.189.150.30";
$username = "ONA";
$password = "aqCGp?wW2c*Xz9V-";
$dbname = "Shortcuts";

// Define the run type
$runType = $argv[1];



//////////////
// Runtime //
////////////


// update terminal
echo "The run type for this job is: ".$runType." \n";

// Create MySQL connection
$MySQLconn = mysqli_connect($servername, $username, $password, $dbname);

// Check MySQL connection
if (!$MySQLconn) {
    die("MySQL connection error" . mysqli_connect_error());
}
echo "Connected successfully to MySQL Server \n";

// Microsoft SQL Server 
$serverName = "ORBE-NA\SHORTCUTSPOS"; //serverName\instanceName

// Since UID and PWD are not specified in the $connectionInfo array,
// The connection will be attempted using Windows Authentication.
$connectionInfo = array( "Database"=>"ShortcutsPOS", 'ReturnDatesAsStrings'=>true);
$conn = sqlsrv_connect( $serverName, $connectionInfo);

if( $conn ) {
     echo "Local MS SQL Shortcuts Connection established. \n";
}else{
     echo "MS SQL Connection could not be established \n";
     die( print_r( sqlsrv_errors(), true));
}

// where this is not an hourly run  - eg daily run, then pick up everything
if ($runType != 'hourly') {

     saleTransactionTable();
     appointmentTable();
     appointmentRecurTable();
     clientsTable();
     employeeSiteTable();
     paymentsByDateClient();
     saleProductByDate();
     saleServiceByDate();
     saleTransactionLineDiscount();
     saleTransactionPayment();
     saleServiceSiteReport();
     giftCertificates();
}

// Run the commands - only these get run hourly
salesTransactionLines();
appointmentAllTable();

// output to terminal
echo("Finished Run");




/////////////////
// Functions //
///////////////

function salesTransactionLines()
{
     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;

     // Query Sale Transaction Lines & Load to MySQL Database
     $sql = "SELECT
          TransactionDate,
          TransactionNumber,
          TransactionLineId,
          SiteName,
          SaleNumber,
          EmployeeId,
          EmployeeFirstName,
          EmployeeSurname,
          EmployeeAlias,
          ClientId,
          ClientFirstName,
          ClientSurname,
          TransactionType,
          TransactionTypeStringCode,
          ItemType,
          ItemTypeStringCode,
          ItemSubType,
          ItemSubTypeStringCode,
          ItemId,
          ItemBlockNumber,
          ItemName,
          ItemQuantity,
          IsExternalGiftCard,
          TaxId,
          TaxName,
          TaxRate,
          DiscountId,
          DiscountName,
          LineDiscountIncTaxAmount,
          LineDiscountExTaxAmount,
          LineIncTaxAmount,
          LineExTaxAmount,
          TaxAmount,
          UnitCostAmount,
          LineCostAmount,
          EmployeeAccruedPoints,
          VoidStatusCode,
          VoidStatusStringCode  
     FROM dbo.scvSaleTransactionLine WHERE TransactionDate >= dateadd(dd,-$dd,getdate())";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Sale Transaction Lines Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

     $sqlSaleTransLines = "REPLACE INTO SaleTransactionLine(
               TransactionDate,
               TransactionNumber,
               TransactionLineId,
               SiteName,
               SaleNumber,
               EmployeeId,
               EmployeeFirstName,
               EmployeeSurname,
               EmployeeAlias,
               ClientId,
               ClientFirstName,
               ClientSurname,
               TransactionType,
               TransactionTypeStringCode,
               ItemType,
               ItemTypeStringCode,
               ItemSubType,
               ItemSubTypeStringCode,
               ItemId,
               ItemBlockNumber,
               ItemName,
               ItemQuantity,
               IsExternalGiftCard,
               TaxId,
               TaxName,
               TaxRate,
               DiscountId,
               DiscountName,
               LineDiscountIncTaxAmount,
               LineDiscountExTaxAmount,
               LineIncTaxAmount,
               LineExTaxAmount,
               TaxAmount,
               UnitCostAmount,
               LineCostAmount,
               EmployeeAccruedPoints,
               VoidStatusCode,
               VoidStatusStringCode
     )
     values (
          '". $row['TransactionDate']."',
          '". $row['TransactionNumber']."',
          '". $row['TransactionLineId']."',
          '". $row['SiteName']."',
          '". $row['SaleNumber']."',
          '". $row['EmployeeId']."',
          '". $row['EmployeeFirstName']."',
          '". $row['EmployeeSurname']."',
          '". $row['EmployeeAlias']."',
          '". $row['ClientId']."',
          '". $row['ClientFirstName']."',
          '". $row['ClientSurname']."',
          '". $row['TransactionType']."',
          '". $row['TransactionTypeStringCode']."',
          '". $row['ItemType']."',
          '". $row['ItemTypeStringCode']."',
          '". $row['ItemSubType']."',
          '". $row['ItemSubTypeStringCode']."',
          '". $row['ItemId']."',
          '". $row['ItemBlockNumber']."',
          '". $row['ItemName']."',
          '". $row['ItemQuantity']."',
          '". $row['IsExternalGiftCard']."',
          '". $row['TaxId']."',
          '". $row['TaxName']."',
          '". $row['TaxRate']."',
          '". $row['DiscountId']."',
          '". $row['DiscountName']."',
          '". $row['LineDiscountIncTaxAmount']."',
          '". $row['LineDiscountExTaxAmount']."',
          '". $row['LineIncTaxAmount']."',
          '". $row['LineExTaxAmount']."',
          '". $row['TaxAmount']."',
          '". $row['UnitCostAmount']."',
          '". $row['LineCostAmount']."',
          '". $row['EmployeeAccruedPoints']."',
          '". $row['VoidStatusCode']."',
          '". $row['VoidStatusStringCode']."'
          )";
     mysqli_query($MySQLconn, $sqlSaleTransLines);

     }

     echo("Sale Transaction Lines Completed \n");

}

function saleTransactionTable()
{
     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;

     // Query Sale Transaction & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvSaleTransaction WHERE TransactionDate >= dateadd(dd,-$dd,getdate())";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Sale Transaction Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          $sqlSaleTrans = "REPLACE INTO SaleTransaction(
                    TransactionDate,
                    TransactionNumber,
                    SaleNumber,
                    CustomerId,
                    IsWalkInCustomer,
                    IsCustomerRebooked,
                    TotalSaleTaxAmount,
                    TotalReturnSaleTaxAmount,
                    IsReturnExchange,
                    TerminalId,
                    TerminalName,
                    OperatorEmployeeId,
                    GroupSaleId,
                    VoidStatusCode,
                    VoidStatusStringCode,
                    VoidedGroupSaleId,
                    VoidedTransactionNumber,
                    AgeGroupId,
                    AgeGroupName,
                    GenderCode,
                    GenderStringCode,
                    PostCode,
                    ReferralMethodId,
                    ReferralMethodName,
                    ReferredByClientId
               )
          values (
               '". $row['TransactionDate']."',
               '". $row['TransactionNumber']."',
               '". $row['SaleNumber']."',
               '". $row['CustomerId']."',
               '". $row['IsWalkInCustomer']."',
               '". $row['IsCustomerRebooked']."',
               '". $row['TotalSaleTaxAmount']."',
               '". $row['TotalReturnSaleTaxAmount']."',
               '". $row['IsReturnExchange']."',
               '". $row['TerminalId']."',
               '". $row['TerminalName']."',
               '". $row['OperatorEmployeeId']."',
               '". $row['GroupSaleId']."',
               '". $row['VoidStatusCode']."',
               '". $row['VoidStatusStringCode']."',
               '". $row['VoidedGroupSaleId']."',
               '". $row['VoidedTransactionNumber']."',
               '". $row['AgeGroupId']."',
               '". $row['AgeGroupName']."',
               '". $row['GenderCode']."',
               '". $row['GenderStringCode']."',
               '". $row['PostCode']."',
               '". $row['ReferralMethodId']."',
               '". $row['ReferralMethodName']."',
               '". $row['ReferredByClientId']."'
               )";
          mysqli_query($MySQLconn, $sqlSaleTrans);

     }
     echo("Sale Transaction Completed \n");

}

function appointmentTable()
{

     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;

     // Query Appointment & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvAppointment WHERE AppointmentDate >= dateadd(dd,-$dd,getdate())";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Appointments Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          $sqlAppt = "REPLACE INTO Appointment(
                    AppointmentId,
                    SiteName,
                    AppointmentDate,
                    CustomerId,
                    CustomerFirstName,
                    CustomerLastName,
                    EmployeeId,
                    EmployeeFirstName,
                    EmployeeSurname,
                    EmployeeAlias,
                    ServiceId,
                    ServiceName,
                    BlockId,
                    BlockName,
                    DurationMinutes,
                    IsNewClient,
                    ResourceId,
                    ResourceName,
                    TagId,
                    TagName
               )
          values (
               '". $row['AppointmentId']."',
               '". $row['SiteName']."',
               '". $row['AppointmentDate']."',
               '". $row['CustomerId']."',
               '". $row['CustomerFirstName']."',
               '". $row['CustomerLastName']."',
               '". $row['EmployeeId']."',
               '". $row['EmployeeFirstName']."',
               '". $row['EmployeeSurname']."',
               '". $row['EmployeeAlias']."',
               '". $row['ServiceId']."',
               '". $row['ServiceName']."',
               '". $row['BlockId']."',
               '". $row['BlockName']."',
               '". $row['DurationMinutes']."',
               '". $row['IsNewClient']."',
               '". $row['ResourceId']."',
               '". $row['ResourceName']."',
               '". $row['TagId']."',
               '". $row['TagName']."'
               )";
          mysqli_query($MySQLconn, $sqlAppt);

     }
     echo("Appointments Completed \n");

}

function appointmentAllTable()
{
     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;

     // Query Appointment All & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvAppointmentAll WHERE AppointmentDate >= dateadd(dd,-$dd,getdate())";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Appointments All Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {


          $sqlApptAll = "REPLACE INTO AppointmentAll(
                    AppointmentId,
                    AppointmentDate,
                    AppointmentTime,
                    CustomerId,
                    EmployeeId,
                    ServiceId,
                    AppointmentNumber,
                    BlockNumber,
                    ServiceBlockPrice,
                    OriginalPrepaymentAmount,
                    RemainingPrepaymentAmount,
                    DurationMinutes,
                    IsDeletedAppointment,
                    IsCancellation,
                    IsNoShow,
                    IsNormalStatus,
                    IsArrived,
                    IsCheckedOut,
                    IsWebBooking,
                    IsNewClient,
                    IsPrepaid
               )
          values (
               '". $row['AppointmentId']."',
               '". $row['AppointmentDate']."',
               '". $row['AppointmentTime']."',
               '". $row['CustomerId']."',
               '". $row['EmployeeId']."',
               '". $row['ServiceId']."',
               '". $row['AppointmentNumber']."',
               '". $row['BlockNumber']."',
               '". $row['ServiceBlockPrice']."',
               '". $row['OriginalPrepaymentAmount']."',
               '". $row['RemainingPrepaymentAmount']."',
               '". $row['DurationMinutes']."',
               '". $row['IsDeletedAppointment']."',
               '". $row['IsCancellation']."',
               '". $row['IsNoShow']."',
               '". $row['IsNormalStatus']."',
               '". $row['IsArrived']."',
               '". $row['IsCheckedOut']."',
               '". $row['IsWebBooking']."',
               '". $row['IsNewClient']."',
               '". $row['IsPrepaid']."'
               )";
          mysqli_query($MySQLconn, $sqlApptAll);

     }
     echo("Appointments All Completed \n");
     
}

function appointmentRecurTable()
{
     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;

     // Query Appointment Recur All & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvAppointmentRecurAll WHERE AppointmentDate >= dateadd(dd,-$dd,getdate())";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Appointments Recur All Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {


          $sqlApptRecurAll = "REPLACE INTO AppointmentRecurAll(
               AppointmentId,
               RecurAppointmentId,
               AppointmentDate,
               RecurAppointmentDate,
               AppointmentTime,
               CustomerId,
               EmployeeId,
               ServiceId,
               AppointmentNumber,
               BlockNumber,
               ServiceBlockPrice,
               OriginalPrepaymentAmount,
               RemainingPrepaymentAmount,
               DurationMinutes,
               IsDeletedAppointment,
               IsCancellation,
               IsNoShow,
               IsNormalStatus,
               IsPrepaid,
               id
               )
          values (
               '". $row['AppointmentId']."',
               '". $row['RecurAppointmentId']."',
               '". $row['AppointmentDate']."',
               '". $row['RecurAppointmentDate']."',
               '". $row['AppointmentTime']."',
               '". $row['CustomerId']."',
               '". $row['EmployeeId']."',
               '". $row['ServiceId']."',
               '". $row['AppointmentNumber']."',
               '". $row['BlockNumber']."',
               '". $row['ServiceBlockPrice']."',
               '". $row['OriginalPrepaymentAmount']."',
               '". $row['RemainingPrepaymentAmount']."',
               '". $row['DurationMinutes']."',
               '". $row['IsDeletedAppointment']."',
               '". $row['IsCancellation']."',
               '". $row['IsNoShow']."',
               '". $row['IsNormalStatus']."',
               '". $row['IsPrepaid']."',
               '". $row['AppointmentId'].$row['RecurAppointmentId']."'
               )";
          mysqli_query($MySQLconn, $sqlApptRecurAll);

     }
     echo("Appointments Recur All Completed \n");

}

function clientsTable()
{
     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;

     // Query Clients & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvClient";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Clients Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          $sqlClient = "REPLACE INTO Client(
               ClientId,
               FirstName,
               Surname,
               NameTitle,
               IsActive,
               FirstVisitDate,
               LastVisitDate,
               AddressLine1,
               AddressLine2,
               AddressSuburb,
               AddressState,
               PostCode,
               HomePhone,
               BusinessPhone,
               MobilePhone,
               FaxPhone,
               Email,
               ClubId,
               ClubName,
               BirthDay,
               BirthMonth,
               BirthYear,
               GenderId,
               GenderStringCode,
               IsClientChild
               )
          values (
               '". $row['ClientId']."',
               '". $row['FirstName']."',
               '". $row['Surname']."',
               '". $row['NameTitle']."',
               '". $row['IsActive']."',
               '". $row['FirstVisitDate']."',
               '". $row['LastVisitDate']."',
               '". $row['AddressLine1']."',
               '". $row['AddressLine2']."',
               '". $row['AddressSuburb']."',
               '". $row['AddressState']."',
               '". $row['PostCode']."',
               '". $row['HomePhone']."',
               '". $row['BusinessPhone']."',
               '". $row['MobilePhone']."',
               '". $row['FaxPhone']."',
               '". $row['Email']."',
               '". $row['ClubId']."',
               '". $row['ClubName']."',
               '". $row['BirthDay']."',
               '". $row['BirthMonth']."',
               '". $row['BirthYear']."',
               '". $row['GenderId']."',
               '". $row['GenderStringCode']."',
               '". $row['IsClientChild']."'
               )";
          mysqli_query($MySQLconn, $sqlClient);


     }


     echo("Client Completed \n");

}

function employeeSiteTable()
{
     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;
     
     // Query Employees & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvEmployeeSite";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Employees Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          $sqlEmployee = "REPLACE INTO EmployeeSite(
               EmployeeId,
               FirstName,
               Surname,
               EmployeeAlias,
               SiteName,
               LevelId,
               LevelName,
               IsActive
               )
          values (
               '". $row['EmployeeId']."',
               '". $row['FirstName']."',
               '". $row['Surname']."',
               '". $row['EmployeeAlias']."',
               '". $row['SiteName']."',
               '". $row['LevelId']."',
               '". $row['LevelName']."',
               '". $row['IsActive']."'
               )";
          mysqli_query($MySQLconn, $sqlEmployee);

     }
     echo("Employees Completed \n");

}

function paymentsByDateClient()
{

     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;

     // Query Payments By Date Client & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvPaymentsByDateClient WHERE PaymentDate >= dateadd(dd,-$dd,getdate())";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Payments By Date Client Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          $PaymentsByDateClient = "REPLACE INTO PaymentsByDateClient(
               PaymentDate,
               ClientId,
               SiteName,
               ClientFirstName,
               ClientSurname,
               ClientGenderId,
               ClientGenderStringCode,
               IsClientChild,
               PaymentAmount,
               TicketCount,
               id
               )
          values (
               '". $row['PaymentDate']."',
               '". $row['ClientId']."',
               '". $row['SiteName']."',
               '". $row['ClientFirstName']."',
               '". $row['ClientSurname']."',
               '". $row['ClientGenderId']."',
               '". $row['ClientGenderStringCode']."',
               '". $row['IsClientChild']."',
               '". $row['PaymentAmount']."',
               '". $row['TicketCount']."',
               '". $row['PaymentDate'].$row['ClientId'].$row['PaymentAmount']."'
               )";
          mysqli_query($MySQLconn, $PaymentsByDateClient);

     }
     echo("Payments By Date Client Completed \n");

}

function saleProductByDate()
{
     
     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;
     
     // Query Sale Product By Date & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvSalesProductByDate WHERE SaleDate >= dateadd(dd,-$dd,getdate())";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Sale Product by Date Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          $SaleProductByDate = "REPLACE INTO SalesProductByDate(
               SaleDate,
               ProductId,
               SiteName,
               ProductName,
               CategoryId,
               CategoryName,
               SubCategoryId,
               SubCategoryName,
               ManufacturerId,
               ManufacturerName,
               LineId,
               LineName,
               ItemQuantity,
               SalesIncTaxAmount,
               SalesExTaxAmount,
               id
               )
          values (
               '". $row['SaleDate']."',
               '". $row['ProductId']."',
               '". $row['SiteName']."',
               '". $row['ProductName']."',
               '". $row['CategoryId']."',
               '". $row['CategoryName']."',
               '". $row['SubCategoryId']."',
               '". $row['SubCategoryName']."',
               '". $row['ManufacturerId']."',
               '". $row['ManufacturerName']."',
               '". $row['LineId']."',
               '". $row['LineName']."',
               '". $row['ItemQuantity']."',
               '". $row['SalesIncTaxAmount']."',
               '". $row['SalesExTaxAmount']."',
               '". $row['SaleDate'].$row['ProductId']."'
               )";
          mysqli_query($MySQLconn, $SaleProductByDate);

     }
     echo("Sale Product By Date Completed \n");

}

function saleServiceByDate()
{

     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;
     
     // Query Sale Service By Date & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvSalesServiceByDate WHERE SaleDate >= dateadd(dd,-$dd,getdate())";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Sale Service by Date Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          $SaleServiceByDate = "REPLACE INTO SalesServiceByDate(
               SaleDate,
               ServiceId,
               SiteName,
               ServiceName,
               CategoryId,
               CategoryName,
               ItemQuantity,
               SalesIncTaxAmount,
               SalesExTaxAmount,
               id
               )
          values (
               '". $row['SaleDate']."',
               '". $row['ServiceId']."',
               '". $row['SiteName']."',
               '". $row['ServiceName']."',
               '". $row['CategoryId']."',
               '". $row['CategoryName']."',
               '". $row['ItemQuantity']."',
               '". $row['SalesIncTaxAmount']."',
               '". $row['SalesExTaxAmount']."',
               '". $row['SaleDate'].$row['ServiceId']."'
               )";
          mysqli_query($MySQLconn, $SaleServiceByDate);

     }
     echo("Sale Service By Date Completed \n");

}

function saleTransactionLineDiscount()
{
     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;

     // Query Sale Transaction Line Discount & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvSaleTransactionLineDiscount";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Sale Transaction Line Discount Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          $TransactionLineDiscount = "REPLACE INTO SaleTransactionLineDiscount(
               SaleTransactionLineDiscountId,
               SaleTransactionLineId,
               ItemId,
               ItemName,
               DiscountSourceCode,
               DiscountSourceStringCode,
               PromotionId,
               PromotionName,
               DiscountId,
               DiscountName,
               ClubId,
               ClubName,
               DiscountPercentage,
               DiscountIncTaxAmount,
               DiscountExTaxAmount,
               DiscountItemCount,
               IsEligibilityItem
               )
          values (
               '". $row['SaleTransactionLineDiscountId']."',
               '". $row['SaleTransactionLineId']."',
               '". $row['ItemId']."',
               '". $row['ItemName']."',
               '". $row['DiscountSourceCode']."',
               '". $row['DiscountSourceStringCode']."',
               '". $row['PromotionId']."',
               '". $row['PromotionName']."',
               '". $row['DiscountId']."',
               '". $row['DiscountName']."',
               '". $row['ClubId']."',
               '". $row['ClubName']."',
               '". $row['DiscountPercentage']."',
               '". $row['DiscountIncTaxAmount']."',
               '". $row['DiscountExTaxAmount']."',
               '". $row['DiscountItemCount']."',
               '". $row['IsEligibilityItem']."'
               )";
          mysqli_query($MySQLconn, $TransactionLineDiscount);

     }
     echo("Sale Transaction Line Discount Completed \n");

}

function saleTransactionPayment()
{

     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;

     // Query Sale Transaction Payment & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvSaleTransactionPayment WHERE TransactionDate >= dateadd(dd,-$dd,getdate())";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Sale Transaction Payments Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          $SaleTransPmts = "REPLACE INTO SaleTransactionPayment(
                    TransactionDate,
                    TransactionNumber,
                    PaymentLineId,
                    PaymentTypeId,
                    SiteName,
                    SaleNumber,
                    PaymentTypeName,
                    IsGiftCard,
                    PaymentAmount,
                    ClientId,
                    VoidStatusCode,
                    VoidStatusStringCode,
                    id
               )
          values (
               '". $row['TransactionDate']."',
               '". $row['TransactionNumber']."',
               '". $row['PaymentLineId']."',
               '". $row['PaymentTypeId']."',
               '". $row['SiteName']."',
               '". $row['SaleNumber']."',
               '". $row['PaymentTypeName']."',
               '". $row['IsGiftCard']."',
               '". $row['PaymentAmount']."',
               '". $row['ClientId']."',
               '". $row['VoidStatusCode']."',
               '". $row['VoidStatusStringCode']."',
               '". $row['SaleNumber'].$row['PaymentTypeName'].$row['ClientId']."'
               )";
          mysqli_query($MySQLconn, $SaleTransPmts);

     }
     echo("Sale Transaction Payments Completed \n");

}

function saleServiceSiteReport()
{

     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;

     // Query Sale Transaction Payment & Load to MySQL Database
     $sql = "SELECT * FROM dbo.scvServiceSiteReportCategory";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Service Site Report Category Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          $ServiceSiteReportCategory = "REPLACE INTO ServiceSiteReportCategory(
                    ServiceId,
                    ServiceName,
                    IsReportCategory1,
                    ReportCategory1Name,
                    IsReportCategory2,
                    ReportCategory2Name,
                    IsReportCategory3,
                    ReportCategory3Name,
                    IsReportCategory4,
                    ReportCategory4Name,
                    IsReportCategory5,
                    ReportCategory5Name,
                    IsReportCategory6,
                    ReportCategory6Name,
                    IsReportCategory7,
                    ReportCategory7Name,
                    IsReportCategory8,
                    ReportCategory8Name,
                    IsReportCategory9,
                    ReportCategory9Name,
                    IsReportCategory10,
                    ReportCategory10Name
               )
          values (
               '". $row['ServiceId']."',
               '". $row['ServiceName']."',
               '". $row['IsReportCategory1']."',
               '". $row['ReportCategory1Name']."',
               '". $row['IsReportCategory2']."',
               '". $row['ReportCategory2Name']."',
               '". $row['IsReportCategory3']."',
               '". $row['ReportCategory3Name']."',
               '". $row['IsReportCategory4']."',
               '". $row['ReportCategory4Name']."',
               '". $row['IsReportCategory5']."',
               '". $row['ReportCategory5Name']."',
               '". $row['IsReportCategory6']."',
               '". $row['ReportCategory6Name']."',
               '". $row['IsReportCategory7']."',
               '". $row['ReportCategory7Name']."',
               '". $row['IsReportCategory8']."',
               '". $row['ReportCategory8Name']."',
               '". $row['IsReportCategory9']."',
               '". $row['ReportCategory9Name']."',
               '". $row['IsReportCategory10']."',
               '". $row['ReportCategory10Name']."'
               )";
          mysqli_query($MySQLconn, $ServiceSiteReportCategory);

     }
     echo("Service Site Report Category Completed \n");
}

function giftCertificates()
{
     // Bring in global variables
     global $conn;
     global $MySQLconn;
     global $dd;
     
     // Query Gift Certificates & Load to MySQL Database
     $sql = "SELECT * FROM dbo.giftCertificate WHERE [Transaction Date] >= dateadd(dd,-$dd,getdate())";
     $stmt = sqlsrv_query( $conn, $sql );
     if( $stmt === false) {
     die( print_r( sqlsrv_errors(), true) );
     }

     echo("Gift Certificates Collected Successfully \n");

     while( $row = sqlsrv_fetch_array( $stmt, SQLSRV_FETCH_ASSOC) ) {

          // generate uniqueId for this gift certificate
          $uniqueId = $row['CertificateID'].'--'.$row['HistoryID'].'--'.$row['Transaction Number'];

          $sqlGiftCertificates = "REPLACE INTO giftCertificates(
               uniqueId,
               CertificateID,
               transactionDate,
               transactionNumber,
               sundryId,
               variance,
               redeemed,
               expiryDate,
               historyId
               )
          values (
               '". $uniqueId."',
               '". $row['CertificateID']."',
               '". $row['Transaction Date']."',
               '". $row['Transaction Number']."',
               '". $row['SundryID']."',
               '". $row['Variance']."',
               '". $row['Redeemed']."',
               '". $row['ExpiryDate']."',
               '". $row['HistoryID']."'
               )";
          mysqli_query($MySQLconn, $sqlGiftCertificates);

     }
     echo("Gift Certificates Completed \n");

}

?>