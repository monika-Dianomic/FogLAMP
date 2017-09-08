import { Component, OnInit, ViewChild } from '@angular/core';
import { SchedulesService, AlertService } from '../services/index';

import { ModalComponent } from '../modal/modal.component';
import { UpdateModalComponent } from '../update-modal/update-modal.component';

enum weekDays {
    Monday = 1,
    Tuesday = 2,
    Wednesday = 3,
    Thursday = 4,
    Friday = 5,
    Saturday = 6,
    Sunday = 7
}


@Component({
  selector: 'app-scheduled-process',
  templateUrl: './scheduled-process.component.html',
  styleUrls: ['./scheduled-process.component.css']
})
export class ScheduledProcessComponent implements OnInit {
  public scheduleData = []
  public scheduleProcess = []
  public scheduleType = []
  public tasksData = []
  public days = [];

  // To handle field validtion on UI
  public invalidRepeat: boolean = false
  public invalidTime: boolean = false

  // Default selected schedule type is STARTUP = 1
  public selected_schedule_type: Number = 1;

  // Object to hold schedule id to delete 
  public childData: any;
  public updateScheduleData: any;

  @ViewChild(ModalComponent) child: ModalComponent;
  @ViewChild(UpdateModalComponent) updateModal: UpdateModalComponent;
  constructor(private schedulesService: SchedulesService, private alertService: AlertService) { }

  ngOnInit() {
    this.days = Object.keys(weekDays).map(key => weekDays[key]).filter(value => typeof value === 'string') as string[];
    this.getScheduleType()
    this.getSchedules()
    this.getSchedulesProcesses()
    this.getLatestTasks()
    
    this.updateScheduleData = {
      schedule_process: this.scheduleProcess,
      schedule_type: this.scheduleType,
      days: this.days
    }
  }

  
  public getScheduleType(): void {
    this.schedulesService.getScheduleType().
      subscribe(
      data => {
        this.scheduleType = data.schedule_type
        console.log(this.scheduleType)
      },
      error => { console.log("error", error) })
  }

  public getSchedules(): void {
    this.scheduleData = [];
    this.schedulesService.getSchedules().
      subscribe(
      data => {
        this.scheduleData = data.schedules
        console.log("This is the getSchedule ", data.schedules)
      },
      error => { console.log("error", error) });
  }

  public getSchedulesProcesses(): void {
    this.scheduleProcess = [];
    this.schedulesService.getScheduledProcess().
      subscribe(
      data => {
        this.scheduleProcess = data.processes
        console.log("This is the getScheduleProcess ", this.scheduleProcess)
      },
      error => { console.log("error", error) });
  }

  public getLatestTasks(): void {
    this.tasksData = [];
    this.schedulesService.getLatestTask().
      subscribe(
      data => {
        this.tasksData = data.tasks;
        console.log("Tasks data", data.tasks)
      },
      error => { console.log("error", error) })
  }

  /**
   *  To set schedule type key globally for required field handling on UI
   * @param value  
   */
  public setScheduleTypeKey(value) {
    this.selected_schedule_type = value;
  }

  public createSchedule() {
    let schedule_name = <HTMLInputElement>document.getElementById("name")
    let schedule_process = <HTMLSelectElement>document.getElementById("process")
    let schedule_type = <HTMLSelectElement>document.getElementById("type")

    let exclusive_state = <HTMLInputElement>document.getElementById("exclusive")

    let repeat_interval_fld = <HTMLInputElement>document.getElementById("repeat")
    var repeat_time = repeat_interval_fld.value != '' ? this.converTimeToSec(repeat_interval_fld.value) : 0
    // check if time is in valid range
    this.invalidRepeat = this.not_between(repeat_time)
    if (this.invalidRepeat) {
      return;
    }

    /**
     *  "schedule_type": [
     *      {"index": 1, "name": "STARTUP"},
     *      {"index": 2,"name": "TIMED"},
     *      {"index": 3,"name": "INTERVAL"},
     *      {"index": 4,"name": "MANUAL"}]
     *      For schedule type 'TIMED', show 'Day' and 'TIME' field on UI
     */
    if (this.selected_schedule_type == 2) {
      var day_fld = <HTMLSelectElement>document.getElementById("day")
      var day = day_fld.value

      var time_fld = <HTMLInputElement>document.getElementById("time")
      var scheduled_time = time_fld.value != '' ? this.converTimeToSec(time_fld.value) : 0

      // check if time is in valid range
      this.invalidTime = this.not_between(scheduled_time)
      if (this.invalidTime) {
        return;
      }
    }

    var payload = {
      "name": schedule_name.value,
      "process_name": schedule_process.value,
      "type": schedule_type.value,
      "repeat": repeat_time,
      "day": day,
      "time": scheduled_time,
      "exclusive": exclusive_state.checked
    }
    this.schedulesService.createSchedule(payload).
      subscribe(
      data => {
        this.getSchedules()
      },
      error => { console.log("error", error) })
  }

  /**
   * To check supplied time range 
   * @param time in seconds
   */
  public not_between(time) {
    // To check if Time in 00:00:01, 23:59:59 range
    return time == 0 || time >= 86400
  }

  // Convert time in seconds 
  converTimeToSec(timeValue) {
    var repeatTime = timeValue.split(':')
    var seconds = (+repeatTime[0]) * 60 * 60 + (+repeatTime[1]) * 60 + (+repeatTime[2])
    return seconds;
  }

  editSchedule(id) {
    console.log("Edit schedule", id)
    this.updateScheduleData = {
      id: id,
      schedule_process: this.scheduleProcess,
      schedule_type: this.scheduleType,
      days: this.days 
    }
    console.log("update: " , this.updateScheduleData);
    
    this.updateModal.toggleModal(true)
  }

  /**
   * To reload schedule list after deletion of a schedule
   * @param notify  
   */
  onNotify() {
    this.getSchedules();
  }

  /**
   * open modal dialog
   * @param id  schedule id to delete
   */
  openModal(id) {
    // call child component method to toggle modal
    this.child.toggleModal(true)
    this.childData = {
      id: id
    }
  }
}