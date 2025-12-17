<?php

// Connect to database
$servername = "206.189.150.30";
$username = "ONA";
$password = "aqCGp?wW2c*Xz9V-";

try {
  $conn = new PDO("mysql:host=$servername;dbname=Shortcuts", $username, $password);
  // set the PDO error mode to exception
  $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
  echo "Connected successfully"; 
} catch(PDOException $e) {
  echo "Connection failed: " . $e->getMessage();
}

// Select Sales Transaction Line Data
$TransactionLines = $conn->prepare("SELECT TransactionDate, EmployeeId, EmployeeFirstName, ClientId, CONCAT(TransactionDate,ClientId) AS UniqueId, ClientFirstName, ClientSurname, TransactionLineId, ItemTypeStringCode, ItemQuantity, ROUND(LineIncTaxAmount,2) AS LineIncTaxAmount FROM SaleTransactionLine WHERE VoidStatusStringCode = 'VoidStatus.Normal' AND DATE_FORMAT(TransactionDate, '%Y-%m-01') >= DATE_FORMAT(NOW(), '%Y-%m-01') -INTERVAL 2 DAY ");
$TransactionLines ->execute();
$result = $TransactionLines->fetchAll();

// Group transactions by UniqueId
$GroupResult = group_by("UniqueId",$result);

// Identify keys of grouped array (which are UniqueId)
$ArrayKeys = array_keys($GroupResult);

// loop through unique array keys
for ($i1=0; $i1 <count($GroupResult) ; $i1++) { 

  // Identify the unique transaction array in this loop
  $UniqueArray = $GroupResult[$ArrayKeys[$i1]];

  // Declare Retail Units as a blank array
  $RetailUnits = [];

  // Loop through transaction lines within this UniqueId to see of the transaction is a product
  for ($i3=0; $i3 <count($UniqueArray) ; $i3++) { 

    // find Product transactions
    if ( $UniqueArray[$i3]['ItemTypeStringCode'] == "ItemType.Product" ) {

      // Add Product item quantities to retail units
      $RetailUnits[] = $UniqueArray[$i3]['ItemQuantity'];
    }
  }

  // Identify basic components for the output
  $TransactionDate = $UniqueArray[0]['TransactionDate'];
  $ClientId = $UniqueArray[0]['ClientId'];
  $ClientFirstName = $UniqueArray[0]['ClientFirstName'];
  $ClientSurname = $UniqueArray[0]['ClientSurname'];
  $LineIncTaxAmount = array_sum(array_column( $UniqueArray,'LineIncTaxAmount'));
  $TotalRetailUnits = array_sum($RetailUnits);
  $BasketSize = count( $UniqueArray );
  $Id = $ArrayKeys[$i1];
  $EmployeeConsolidated = []; // Declare EmployeeConsolidated as a blank array to be populated below

  // Now start the process to find the staff member that has the max income assigned for this Transaction.  Group unique transaction by staff member 
  $StaffTransGroup = group_by('EmployeeId',$UniqueArray);

  for ($i2=0; $i2 <count( $StaffTransGroup ) ; $i2++) { 

      // Identify Array Keys position
      $Position = array_keys( $StaffTransGroup)[$i2];

      // Sum the transaction value for each unique employee who serviced the client
      $value = array_sum(array_column( $StaffTransGroup[$Position],'LineIncTaxAmount') );
      $EmpName = $StaffTransGroup[$Position][0]['EmployeeFirstName'];
      
      // Create output array grouping total income for the transaction by employee
      $EmployeeConsolidated[] = [
        "LineIncTaxAmount" => "$value",
        "EmployeeId" => "$Position",
        "EmployeeName" => "$EmpName"
      ];

  }

  // Identify the maximum income amount by employee for this unique transaction
  $MaxAmount = max( array_column( $EmployeeConsolidated,'LineIncTaxAmount') );

  // Quote blank variables to be completed with the IF statement below
  $EmployeeName = '';
  $EmployeeId = '';

  // Find the employee with the max income for this transaction
  foreach ( $EmployeeConsolidated as $row ) {

    // Boolean if Employee matches the max amount
    $Variable = $row['LineIncTaxAmount'] == $MaxAmount;

    // Identify Employee details where they are equal to the max
    if ($Variable) {
      $EmployeeId = $row['EmployeeId'];
      $EmployeeName = $row['EmployeeName'];
      
    }

  }

  // State output array to then be written into the database
  $OutputArray = [
    "TransactionDate" => "$TransactionDate",
    "ClientId" => "$ClientId",
    "ClientFirstName" => "$ClientFirstName",
    "ClientSurname" => "$ClientSurname",
    "LineIncTaxAmount" => "$LineIncTaxAmount",
    "EmployeeId" => "$EmployeeId",
    "EmployeeName" => "$EmployeeName",
    "BasketSize" => "$BasketSize",
    "UniqueId" => "$Id",
    "RetailUnits" => "$TotalRetailUnits",
  ];

  // Insert values into MySQL database
  $sql = "REPLACE INTO SaleTransactionLineByEmployee (TransactionDate, ClientId, ClientFirstName, ClientSurname, LineIncTaxAmount, EmployeeId, EmployeeName,BasketSize, RetailUnits, UniqueId) VALUES (:TransactionDate, :ClientId, :ClientFirstName, :ClientSurname, :LineIncTaxAmount, :EmployeeId, :EmployeeName, :BasketSize, :RetailUnits, :UniqueId)";
  $stmt= $conn->prepare($sql);
  $stmt->execute($OutputArray);

} 
  
echo " \n Done!";




////////////////
// Functions //
//////////////

function group_by($key, $data) {
  $result = array();

  foreach($data as $val) {
      if(array_key_exists($key, $val)){
          $result[$val[$key]][] = $val;
      }else{
          $result[""][] = $val;
      }
  }

  return $result;
}