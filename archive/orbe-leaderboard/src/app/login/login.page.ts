import { Component, OnInit, ViewChild } from '@angular/core';
import { NgForm } from '@angular/forms';
import { DataService } from '../services/data.service';
import { Router } from '@angular/router';
import { AlertController } from '@ionic/angular';

@Component({
  selector: 'app-login',
  templateUrl: './login.page.html',
  styleUrls: ['./login.page.scss'],
})
export class LoginPage implements OnInit {

  // declare access variable for the login form
  @ViewChild('f') loginForm: NgForm;

  // declare variable to track whether to show the login page
  isLoading: boolean = false;

  constructor(
    private dataService: DataService,
    private router:Router,
    private alertController: AlertController
  ) { }

  ngOnInit() {
  }

  async loginError() {
    const alert = await this.alertController.create({
      header: 'Login Unsuccessful',
      subHeader: '',
      message: 'Please try again',
      buttons: ['OK'],
    });

    await alert.present();

  }

  onLogin( ){

    // update the loading variable
    this.isLoading = true;

    // call the dataservice to login a user
    this.dataService.login( this.loginForm.value.username, this.loginForm.value.password ).subscribe(

      // if the response has been successful
      responseData => {

        // set the auth token and token expiry in local storage
        localStorage.setItem('authToken', responseData['token']);
        localStorage.setItem('tokenExpiry', responseData['expiry']);

        // navigate to the home page
        this.router.navigate(['/home']);

        // update the loading variable
        this.isLoading = false;

      },

      // if we have an error
      errorData => {

        // log the error to the console
        console.log(errorData);

        // throw an alert
        this.loginError();

        // update the loading variable
        this.isLoading = false;


      }
    )
  }

}
