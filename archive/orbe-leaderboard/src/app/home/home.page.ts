import { Component, OnDestroy, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { LoadingController } from '@ionic/angular';
import { Subscription } from 'rxjs';
import { DataService } from '../services/data.service';
import { pulseProgress } from './animations';

@Component({
  selector: 'app-home',
  templateUrl: 'home.page.html',
  styleUrls: ['home.page.scss'],
  animations: [pulseProgress]
})
export class HomePage implements OnInit, OnDestroy {

  public progress = 0;

  // animataion variable state
  public pulseProgressVal = 'out';

  // declare an example active staff member array
  staffMembersArray = [ ];
  percentComplete = 0;
  activeStaffMember = {
    'name': '',
    'revenue': {
      'actual': 0,
      'target': 0,
      'progressPercent': 0,
      'flexStyle': '0 0 0%',
      'progressColour': 'success'
    },
    'rebooking': {
      'actual': 0,
      'target': 0,
      'progressPercent': 0,
      'flexStyle': '0 0 0%',
      'progressColour': 'success'
    },
    'retail': {
      'actualUnits': 0,
      'actualRevenue': 0,
      'progressColour': 'success',
    },
    'weekPercentCompleted': 0
  }

  // declare main loop variable
  staffLoop: any;
  exit: boolean = false;

  // declare init load
  staffIndex = 0;

  // tracks whether the screen has been clicked
  screenClicked: boolean = false;

  // subscription to listen for changes in settings
  changesEmitter: Subscription;


  constructor(
    private router: Router,
    private dataService: DataService,
    private loadingCtrl: LoadingController
    )
  {}

  ngOnInit(): void {


    ///////////////////////
    // Get initial data //
    /////////////////////

    // update the loading variable
    this.showLoading( );

    // calls the api to get new leaderboard data
    this.getLeaderboardData( );

    // set the timer again
    this.setTimer( );

    // listen for save changes
    this.changesEmitter = this.dataService.saveChangesEmitter.subscribe( settingsChanged => {

      // when a setting is changed, exit out of the current loop and refresh the API (that functionality is handled in the loopThroughStaff() function)
      this.exit = true;

    });

  }

  ngOnDestroy(): void {

    // unsubscribe from the changes emitter
    this.changesEmitter.unsubscribe();

  }

  onSettingsClick(){
    this.router.navigate(['/settings']);

  }

  async showLoading() {
    const loading = await this.loadingCtrl.create({
      message: 'Loading...'
    });

    loading.present();
  }

  getLeaderboardData(){

    // call the API to get updated leaderboard data
    this.dataService.getLeaderboardData( ).subscribe(

      data => {

        // clear the existing staff members array
        this.staffMembersArray = [ ];

        // get the variables from the response
        let dataset = data['response'];
        let percentComplete = data['percentComplete'];
        let employeeNamesDict = data['employeeNamesDictionary'];

        // loop through each employee
        Object.keys(dataset).forEach(employeeId => {

          // get the employee name
          let employeeName = employeeNamesDict[String(employeeId)] ?? '';


          //////////////
          // Revenue //
          ////////////

          // work out the revenue progress colour
          let revenueProgressColour = "close";
          let revenue = dataset[employeeId]['revenue']['actual'];
          let target = dataset[employeeId]['revenue']['target'];
          let revenueDifference = revenue - target;

          // calculate revenue diff percent - removing the potential for NaN values
          let revenueDiffPerc = 0;
          if(
            typeof revenue !='undefined' &&
            revenue > 0 &&
            typeof target !='undefined' &&
            target > 0
          ){
            revenueDiffPerc = revenue / target
          }

          // update the revenue diff percent to be at worst 0%
          if( revenueDiffPerc < 0 ){
            revenueDiffPerc = 0;
          }

          // get the revenue diff string for css style
          let revDiffString = (revenueDiffPerc * 100).toFixed(0) + "%";

          // get the relative difference taking into account the time of week
          let relativeDiffPerc = -1;
          if(
            typeof revenue !='undefined' &&
            revenue > 0 &&
            typeof target !='undefined' &&
            target > 0 &&
            typeof percentComplete !='undefined' &&
            percentComplete > 0

          ){
            relativeDiffPerc = (revenue / target) - percentComplete
          }

          // if we're behind by more than 20% (relative), then we're in trouble
          if( relativeDiffPerc < -0.20 ){
            revenueProgressColour = 'danger'
          }

          // if we're on track, then happy days
          if( relativeDiffPerc >= 0 ){
            revenueProgressColour = 'success'
          }


          ////////////////
          // Rebooking //
          //////////////

          let rebookingProgressColour = 'close';
          let rebookingActual = dataset[employeeId]['rebooking']['actual'];
          let rebookingTarget = dataset[employeeId]['rebooking']['target'];
          let rebookingDifference = rebookingActual - rebookingTarget;

          // work out the rebooking diff percent (removing NaN values)
          let rebookingDiffPerc = 0;
          if(
              typeof rebookingActual != 'undefined' &&
              rebookingActual > 0 &&
              typeof rebookingTarget != 'undefined' &&
              rebookingTarget > 0
          ){
            rebookingDiffPerc = rebookingActual / rebookingTarget
          }

          // if we're behind by more than 20% (relative), then we're in trouble
          if( rebookingDiffPerc <= 0 ){
            rebookingProgressColour = 'danger'
          }

          // if we're on track, then happy days
          if( rebookingDiffPerc >= 1 ){
            rebookingProgressColour = 'success'
          }

          // limit the difference percent to 100% either way
          if(rebookingDiffPerc < -1){
            rebookingDiffPerc = -1;
          }
          if(rebookingDiffPerc > 1){
            rebookingDiffPerc = 1
          }

          // update the revenue diff percent to be at worst 0%
          if( rebookingDiffPerc < 0 ){
            rebookingDiffPerc = 0;
          }

          // get the revenue diff string for css style
          let rebookingDiffString = (rebookingDiffPerc * 100).toFixed(0) + "%";


          /////////////
          // Retail //
          ///////////

          // get products revenue
          let retailUnitsColour = 'close';
          let productsRevenue = dataset[employeeId]['products']['revenue'];
          let productsUnits = dataset[employeeId]['products']['units'];
          let productUnitsTarget = dataset[employeeId]['products']['unitsTarget'];

          // get the relative difference taking into account the time of week
          let retailRelativeDiffPerc = -1;
          if(
            typeof productsUnits !='undefined' &&
            productsUnits > 0 &&
            typeof productUnitsTarget !='undefined' &&
            productUnitsTarget > 0 &&
            typeof percentComplete !='undefined' &&
            percentComplete > 0

          ){
            retailRelativeDiffPerc = (productsUnits / productUnitsTarget) - percentComplete
          }

          // if we're behind by more than 20% (relative), then we're in trouble
          if( retailRelativeDiffPerc < -0.20 ){
            retailUnitsColour = 'danger'
          }

          // if we're on track, then happy days
          if( retailRelativeDiffPerc >= 0 ){
            retailUnitsColour = 'success'
          }

          // add to the staff members array
          this.staffMembersArray.push({
            'name': employeeName,
            'revenue': {
              'actual': revenue,
              'target': target,
              'progressPercent': revenueDiffPerc,
              'flexStyle': "0 0 " + revDiffString,
              'progressColour': revenueProgressColour
            },
            'rebooking': {
              'actual': rebookingActual,
              'target': rebookingTarget,
              'progressPercent': rebookingDiffPerc,
              'flexStyle': "0 0 " + rebookingDiffString,
              'progressColour': rebookingProgressColour
            },
            'retail': {
              'actualUnits': productsUnits,
              'actualRevenue': productsRevenue,
              'progressColour': retailUnitsColour,
            },
            'weekPercentCompleted': "0 0 " + ( percentComplete * 100).toFixed() + "%"

          });
        });

        // remove the loading message
        this.loadingCtrl.dismiss();

        // update the staff index
        this.staffIndex = 0;

        // loop through each staff member
        this.loopThroughStaff();

      },

      error =>{

        // if we get a 401 error
        if( error['status'] == 401 ){

          // remove the loading message
          this.loadingCtrl.dismiss();

          // remove token details from local storage
          localStorage.removeItem('authToken');
          localStorage.removeItem('tokenExpiry');

          // navigate back to the login screen
          this.router.navigate(['/login']);

        }
      }
    )

  }

   // function that iterates through all employees returned in the array
   loopThroughStaff( ){

    // count the number of staff in the loop
    let numStaff = this.staffMembersArray.length -1; // less 1 because the index starts at 0

    // animate the first staff member in
    this.pulseProgressVal = 'in';

    // update the active staff member to the first person in the index
    this.activeStaffMember = this.staffMembersArray[this.staffIndex];

    // loop through each staff member
    this.staffLoop = setInterval(() => {

      // increment this progress every 100 milliseconds
      this.progress += 0.009;


      /////////////////////
      // Screen Clicked //
      ///////////////////

      // if the screen has been clicked
      if( this.screenClicked ){

        // zoom out of the first staff member (animation affect)
        this.pulseProgressVal = 'out';

        // reset progress
        this.progress = 0;

        // if we've reached the end of the staff list, then rest back to zero.  Otherwise, increment to the next staff member
        if(this.staffIndex >= numStaff){
          this.staffIndex = 0
        } else {
          this.staffIndex ++;
        }

        // set timeout to 1 second to align with the animation time
        setTimeout(() => {

          // zoom in the next staff member (animation affect)
          this.pulseProgressVal = 'in';

          // increment to the next person in the list
          this.activeStaffMember = this.staffMembersArray[this.staffIndex];

        }, 1000);

        // turn the screen clicked variable off
        this.screenClicked = false;


      }


      /////////////////////////////
      // Progress Bar Completed //
      ///////////////////////////

      // Reset the progress bar when it reaches 100%
      // to continuously show the demo
      if (this.progress > 1) {

        // zoom out of the first staff member (animation affect)
        this.pulseProgressVal = 'out';

        // reset progress
        this.progress = 0;

        // if we've reached the end of the staff list, then rest back to zero.  Otherwise, increment to the next staff member
        if(this.staffIndex >= numStaff){
          this.staffIndex = 0
        } else {
          this.staffIndex ++;
        }

        ////////////////////////////////
        // Check Time to Refresh API //
        //////////////////////////////

        // if it's time to get new data
        if( this.exit ){

          // clear all existing setInterval functions
          clearInterval(this.staffLoop);

          // update the loading variable
          this.showLoading( );

          // calls the api to get new leaderboard data
          this.getLeaderboardData( );

          // update the exit variable to false
          this.exit = false;

          // set the timer again
          this.setTimer( );

        }

        // set timeout to 1 second to align with the animation time
        setTimeout(() => {

          // zoom in the next staff member (animation affect)
          this.pulseProgressVal = 'in';

          // increment to the next person in the list
          this.activeStaffMember = this.staffMembersArray[this.staffIndex];

        }, 1000);

      }

    }, 100);


  }

  setTimer( ){

    // wait 10 minutes before setting our timer again
    setTimeout(() => {
      this.exit = true;
    }, 600000); // 10 minutes in milliseconds

  }

  clickScreen(){

    this.screenClicked = true;


  }


}
