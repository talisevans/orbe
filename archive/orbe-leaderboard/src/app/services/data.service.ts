import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from  '@angular/common/http';
import * as _moment from 'moment';
import { Router } from '@angular/router';
import { Subject } from 'rxjs';

export interface Message {
  fromName: string;
  subject: string;
  date: string;
  id: number;
  read: boolean;
}

@Injectable({
  providedIn: 'root'
})
export class DataService {

  // declare a momentJS object
  moment: _moment.Moment = _moment();

  // declare the API base url
  baseURL = "https://orbe-api.patrickbirthday.party";
  // baseURL = "http://127.0.0.1:5000";

  // create a new subject to track save changes so the API can reload
  saveChangesEmitter = new Subject<boolean>();

  constructor(
    private http: HttpClient,
    private router:Router

  ) { }

  // used on the settings page to get staff settings
  getStaffSettings(){

    // make sure we have a valid token
    this.checkTokenValid( );

    // returns basic employee settings
    return this.http.get( this.baseURL+"/get-settings" )

  }

  // used on the home page to populate the main leaderboard dataset
  getLeaderboardData(){

    // make sure we have a valid token
    this.checkTokenValid( );

    // returns basic employee settings
    return this.http.get( this.baseURL+"/leaderboard" )

  }

  // used on the settings page to update staff records
  updateStaffSettings( staffData ){

    // returns basic employee settings
    return this.http.post( this.baseURL+"/update-settings", staffData );

  }

  // function to make sure we have a valid token
  checkTokenValid(){

    // get the auth token
    let authToken = "";
    if( typeof localStorage.getItem('authToken') != 'undefined' ){
      authToken = localStorage.getItem('authToken');
    }

    // get the token expiry
    let tokenExpiry: number = -1;
    if( typeof localStorage.getItem('tokenExpiry') != 'undefined' ){
      tokenExpiry = parseInt(localStorage.getItem('tokenExpiry'));
    }

    // if any of our token challenge criteria are met
    if(
      authToken == null ||
      authToken.length < 1 || // if we don't currently have a token
      tokenExpiry == -1  || // if a token expiry hasn't been set
      tokenExpiry < this.moment.unix() // if the expiry is after the current time
    ){
      this.router.navigate(['/login'])

    }


  }

  // function to login a user and get an access token
  login( username, password ){

    // declare the login string
    let loginString = username+":"+password;

    // set the http login options
    const httpOptions = {
      headers: new HttpHeaders({
        'Content-Type':  'application/json',
        'Authorization': 'Basic ' + btoa( loginString )
      })
    };

    // return the webservice call
    return this.http.get( this.baseURL+"/login", httpOptions )

  }


}
