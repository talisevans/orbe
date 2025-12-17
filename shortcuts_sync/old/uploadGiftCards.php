<?php 

/*

The purpose of this script is to upload new gift vouchers purchased online

Use this link to download the csv file and place the file named 'TransactionExport.csv' in this directory
https://programs.shortcutssoftware.com/console/carddetails.aspx

*/

//MySQL Connection Variables
$servername = "206.189.150.30";
$username = "ONA";
$password = "aqCGp?wW2c*Xz9V-";
$dbname = "Shortcuts";

// Create MySQL connection
$MySQLconn = mysqli_connect($servername, $username, $password, $dbname);

// Collect the csv file as an associative array
$giftCertificateTransactions = MapCSV( __DIR__.'/TransactionExport.csv') ;

// loop through each row in the CSV file to insert into the database
for ($i1=0; $i1 <count( $giftCertificateTransactions ) ; $i1++) { 

    // jump over records that are empty
    if( empty($giftCertificateTransactions[$i1]['GiftCardTransactionId']) ){
        continue;
    }

    // calculate uniqueId
    $uniqueId = $giftCertificateTransactions[$i1]['CardNumber'].'--'.$giftCertificateTransactions[$i1]['GiftCardTransactionId'];

    // prepare SQL replace into statement
    $sql = "

        REPLACE INTO giftCards (
            uniqueId, 
            GiftCardRuleSetName, 
            SiteName, 
            transactionDate, 
            CardNumber, 
            PurchaserFullName, 
            TransactionTypeStringCode, 
            TransactionExTaxAmount, 
            GiftCardTransactionId, 
            CreatedUserName
        )
        VALUES ( ?,?,?,?,?,?,?,?,?,? )

    ";

    $stmt = $MySQLconn->prepare( $sql );
    $stmt->bind_param("ssssssssss", 
        $uniqueId,
        $giftCertificateTransactions[$i1]['GiftCardRuleSetName'],
        $giftCertificateTransactions[$i1]['SiteName'],
        $giftCertificateTransactions[$i1]['SiteTransactionDate'],
        $giftCertificateTransactions[$i1]['CardNumber'],
        $giftCertificateTransactions[$i1]['PurchaserFullName'],
        $giftCertificateTransactions[$i1]['TransactionTypeStringCode'],
        $giftCertificateTransactions[$i1]['TransactionExTaxAmount'],
        $giftCertificateTransactions[$i1]['GiftCardTransactionId'],
        $giftCertificateTransactions[$i1]['CreatedUserName']
    );
    $stmt->execute();
    $stmt->close();
    
}

// update Terminal
echo "gift card data successfullly uploaded! \n";
echo "All done! \n";








////////////////
// Functions //
//////////////

// Transforms CSV into an associative array
function MapCSV($file) 
{
    $rows = array_map('str_getcsv', file($file));
    $header = array_shift($rows);
    $array = array();
    foreach ($rows as $row) {
        
        // Temp array used for storage
        $TempArray = [ ];

        // Loop through each row in the header
        for ( $i1 = 0 ; $i1 < count( $header ) ; $i1++ ) {
            
            // Temp array used to store this value
            $TempVal = "";
            if ( isset( $row[$i1] ) ) {
                $TempVal = $row[$i1];
            }

            // Assign the value for this index
            $TempArray[ $header[$i1] ] = $TempVal;
        }

        // Add it to the final array
        $array[ ] = $TempArray;

    }
    return $array;
}

?>