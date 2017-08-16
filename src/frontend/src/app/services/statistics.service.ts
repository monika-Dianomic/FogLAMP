import { Injectable } from '@angular/core';
import { Http, Response } from '@angular/http'
import { Observable } from 'rxjs/Rx'
import  Utils, {BASE_URL} from './utils'


@Injectable()
export class StatisticsService {

  private GET_STATISTICS = BASE_URL + "statistics"
  private GET_STATISTICS_HISTORY = BASE_URL + "statistics/history?limit=5"

  constructor(private http: Http) { }

  /**
   *    GET  | /foglamp/statistics 
   */
  public getStatistics() {
    return this.http.get(this.GET_STATISTICS)
      .map(response => response.json())
      .catch((error: Response) => Observable.throw(error.json().message || 'Server error'))
  }

  /**
   *  GET | /foglamp/statistics/history 
   */
  public getStatisticsHistory() {
    return this.http.get(this.GET_STATISTICS_HISTORY)
      .map(response => response.json())
      .catch((error: Response) => Observable.throw(error.json().message || 'Server error'))
  }
}
