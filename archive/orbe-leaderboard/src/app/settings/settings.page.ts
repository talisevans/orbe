import { Component, OnInit, ViewChild } from '@angular/core';
import { FormControl, FormGroup, NgForm } from '@angular/forms';
import { Router } from '@angular/router';
import { DataService } from '../services/data.service';
import { LoadingController } from '@ionic/angular';

@Component({
  selector: 'app-settings',
  templateUrl: './settings.page.html',
  styleUrls: ['./settings.page.scss'],
})
export class SettingsPage implements OnInit {

  // declare the staff settings array
  staffDataset = {};
  staffDataArray = [ ];

  // declare a variable for the form
  @ViewChild('f') settingsForm: NgForm;

  // boolean to track whether we're in loading mode
  isLoading: boolean = false;

  constructor(
    private router:Router,
    private DataService: DataService,
    private loadingCtrl: LoadingController
  ) { }

  ngOnInit() {

    // update the loading variable
    this.isLoading = true;

    // clear the staff dataset
    this.staffDataset = [ ];

    // get staff from the dataset
    this.DataService.getStaffSettings().subscribe(

      // when we have a successful response
      data => {

        // update the staff data array
        this.staffDataArray =  data['data'];

        // loop through the staff dataset
        this.staffDataArray.forEach(element => {

          // declare the input key strings
          let targetIncomeString = element['EmployeeId'] + "---targetIncome";
          let targetRebookingString = element['EmployeeId'] + "---targetRebookings";

          // update the staff dataset with any existing values
          this.staffDataset[element['EmployeeId'] + "---targetIncome"] = element['targetIncome'];
          this.staffDataset[element['EmployeeId'] + "---targetRebookings"] = element['targetRebookings'];
          this.staffDataset[element['EmployeeId'] + "---showEmployee"] = element['showEmployee'];


        });

        // update the loading variable
        this.isLoading = false;

      },
      error => {

        // if we get a 401 error
        if( error['status'] == 401 ){

          // remove the loading message
          this.loadingCtrl.dismiss();

          // update the loading variable
          this.isLoading = false;

          // remove token details from local storage
          localStorage.removeItem('authToken');
          localStorage.removeItem('tokenExpiry');

          // navigate back to the login screen
          this.router.navigate(['/login']);

        }

      }
    );


  }

  dashboardNavigate(){
    this.router.navigate(['/home']);
  }

  async showLoading() {
    const loading = await this.loadingCtrl.create({
      message: 'Saving...'
    });

    loading.present();
  }

  onSubmit(){

    this.showLoading();

    // declare a structured dataset to use for our post request
    let structuredDataset = {};

    Object.keys(this.staffDataset).forEach( element => {

      // get the key components
      let keyComponents = element.split("---");
      let employeeId = keyComponents[0];
      let measure = keyComponents[1];

      // add this key to the structured dataset
      if( typeof structuredDataset[employeeId] == 'undefined' ){
        structuredDataset[employeeId] = {};
      }

      // add to the structured dataset
      structuredDataset[employeeId][measure] = this.staffDataset[element];

    });

    // now our structured dataset has been prepared, send it to the API
    this.DataService.updateStaffSettings( structuredDataset ).subscribe(
      responseData => {

        // if our response is a success, then dismiss the loading controller
        if( responseData['response'] == 'success' ){
          this.loadingCtrl.dismiss();

          // reload the api on the home page
          this.DataService.saveChangesEmitter.next(true);
        }

      },
      errorData => {

        console.log(errorData);

      }
    )


  }

}
