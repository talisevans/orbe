import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { RouteReuseStrategy } from '@angular/router';
import { HttpClientModule, HTTP_INTERCEPTORS } from '@angular/common/http';
import { IonicModule, IonicRouteStrategy } from '@ionic/angular';
import { AppComponent } from './app.component';
import { AppRoutingModule } from './app-routing.module';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import {MatCheckboxModule} from '@angular/material/checkbox';
import * as moment from 'moment';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { authInterceptorService } from './services/auth-interceptor.service';
import { DataService } from './services/data.service';


@NgModule({
  declarations: [AppComponent],
  imports: [
    BrowserModule,
    IonicModule.forRoot(),
    AppRoutingModule,
    BrowserAnimationsModule,
    MatCheckboxModule,
    HttpClientModule,
    FormsModule,
    ReactiveFormsModule
  ],
  providers: [
    { provide: RouteReuseStrategy, useClass: IonicRouteStrategy },
    { provide: 'moment', useValue: moment },
    {
      provide: HTTP_INTERCEPTORS,
      useClass: authInterceptorService,
      multi: true,
      deps: [ DataService ]
    }
  ],
  bootstrap: [AppComponent],
})
export class AppModule {}
