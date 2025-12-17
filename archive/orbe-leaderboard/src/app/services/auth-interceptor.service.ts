import { HttpHandler, HttpInterceptor, HttpRequest } from "@angular/common/http";
import { Observable } from "rxjs";
import { DataService } from "./data.service";


export class authInterceptorService implements HttpInterceptor{

  intercept(req: HttpRequest<any>, next: HttpHandler) {

    // if this isn't a request to the login endpoint, then attach the token
    if( req.url.slice(-5) != 'login' ){

      // get the auth token
      const authToken = localStorage.getItem('authToken');
      const tokenExpiry = localStorage.getItem('tokenExpiry');

      const req1 = req.clone({
        headers: req.headers.set('Authorization', 'Bearer ' + authToken ),
      });

      return next.handle(req1);

      // otherwise, don't make any changes
    } else {
      return next.handle(req);
    }



  }
}
