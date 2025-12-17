<?php

//MySQL Connection Variables
$servername = "206.189.150.30";
$username = "ONA";
$password = "aqCGp?wW2c*Xz9V-";
$dbname = "Shortcuts";

// Create MySQL connection
$mysqli = mysqli_connect($servername, $username, $password, $dbname);

$etlDataset = [ ];

$stmt = $mysqli->prepare("
  SELECT
    TransactionDate,
    ClientId,
    SaleTransactionLine.EmployeeId,
    EmployeeFirstName AS EmployeeName,
    CONCAT(ClientFirstName,' ',ClientSurname) AS ClientName,
    ROUND(SUM(LineExTaxAmount)) AS LineAmount,
    ItemName,
    ItemType,
    TransactionLineId
  FROM SaleTransactionLine
  WHERE DATE(SaleTransactionLine.TransactionDate) = DATE('2022-09-03')
  GROUP BY
    TransactionDate,
    ClientId,
    EmployeeName,
    ClientName,
    EmployeeId,
    ItemName,
    ItemType,
    TransactionLineId
");
$stmt->execute();
$result = $stmt->get_result();
while($row = $result->fetch_assoc()) {

    // set the dataset if needed
    if(!isset($etlDataset[$row['ClientId']][$row['TransactionDate']]['employees'][$row['EmployeeId']])){
      $etlDataset[$row['ClientId']][$row['TransactionDate']]['employees'][$row['EmployeeId']] = [ ];
    }

    // set the dataset if needed
    if(!isset($etlDataset[$row['ClientId']][$row['TransactionDate']]['isRebooked'])){
      $etlDataset[$row['ClientId']][$row['TransactionDate']]['isRebooked'] = false;
    }

    // add to the ETL
    $etlDataset[$row['ClientId']][$row['TransactionDate']]['employees'][$row['EmployeeId']][ ] = [
      "clientName" => $row['ClientName'],
      "itemName" => $row['ItemName'],
      "amount" => $row['LineAmount'],
      "EmployeeName" => $row['EmployeeName']
    ];

}
$stmt->close();


$stmt = $mysqli->prepare("
  SELECT
    MAX(DATE(AppointmentDate)) AS appointmentDate,
    CustomerId
  FROM AppointmentAll
  WHERE
    IsCancellation != 1 AND IsDeletedAppointment != 1 AND IsNoShow != 1
    GROUP BY CustomerId
");
$stmt->execute();
$result = $stmt->get_result();
while($row = $result->fetch_assoc()) {

  // if we have this customerId in the dataset
  if( isset($etlDataset[$row['CustomerId']]) ){

    // then loop through each transaction date for this customer within range
    foreach ($etlDataset[$row['CustomerId']] as $transactionDate => $employeesArray) {

      // if the max appointment date is after the transaction date
      $transDate = DateTime::createFromFormat( 'Y-m-d', $transactionDate );
      $apptDate = DateTime::createFromFormat( 'Y-m-d', $row['appointmentDate'] );
      
      // then it's a rebooking
      if( $apptDate > $transDate ){
        $etlDataset[$row['CustomerId']][$transactionDate]['isRebooked'] = true;
        $etlDataset[$row['CustomerId']][$transactionDate]['appointmentDate'] = $row['appointmentDate'];
      }

    }

  }
}
$stmt->close();


// decare the output dataset
$outputDataset = [ ];

foreach ($etlDataset as $clientId => $transactionDatesArray) {
  foreach ($transactionDatesArray as $transactionDate => $employeesArray) {
    
    // is this booking rebooked
    $isRebooked = $employeesArray['isRebooked'];

    // declare variables to find the max employee name
    $employeeValue = 0;
    $maxEmployeeId = "";
    $maxEmployeeName = "";

    ////////////////////////////
    // Find the max employee //
    //////////////////////////

    // loop through all employees
    foreach ($employeesArray['employees'] as $employeeId => $valuesArray) {
      
      // get the total value spent with this employee
      $testValue = 0;
      foreach ($valuesArray as $valueArray) {
        $testValue += $valueArray['amount'];
      }

      // if the amount spent with this employee is more than what we currently have, then update the max employeeId
      if( $testValue > $employeeValue ){

        // update the variables
        $maxEmployeeId = $employeeId;
        $maxEmployeeName = $valueArray['EmployeeName'];
        $employeeValue = $testValue;
      }
      
    }


    /////////////////////////
    // Add to the dataset //
    ///////////////////////

    // loop through all employees
    foreach ($employeesArray['employees'] as $employeeId => $valuesArray) {

      // loop through all employees
      foreach ($valuesArray as $valueArray) {

        // build the output dataset
        $outputDataset[] = [
          "transactionDate" => $transactionDate,
          "clientId" => $clientId,
          "clientName" => $valueArray['clientName'],
          "employeeId" => $employeeId,
          "EmployeeName" => $valueArray['EmployeeName'],
          "itemName" => $valueArray['itemName'],
          "amount" => $valueArray['amount'],
          "isRebooked" => $isRebooked,
          "rebookingLinkedEmployeeId" => $maxEmployeeId,
          "rebookingLinkedEmployeeName" => $maxEmployeeName,
          "rebookedAppointmentDate" => $employeesArray['appointmentDate'] ?? ""
        ];

      }

    }
  }
  
}

// export this dataset to csv
outputCsv( 'rebookings.csv', $outputDataset );



function outputCsv($fileName, $assocDataArray)
{
    if(isset($assocDataArray['0'])){
        $fp = fopen($fileName, 'w');
        fputcsv($fp, array_keys($assocDataArray['0']));
        foreach($assocDataArray AS $values){
            fputcsv($fp, $values);
        }
        fclose($fp);
    }
}