import { Component, OnInit, Input } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { ConfigurationService } from '../services/index';

@Component({
  selector: 'app-configuration-manager',
  templateUrl: './configuration-manager.component.html',
  styleUrls: ['./configuration-manager.component.css']
})
export class ConfigurationManagerComponent implements OnInit {
  public categoryData = [];
  public configurationData = [];
  public isValid = true;
  constructor(private configService: ConfigurationService,
    private route: ActivatedRoute) { }

  ngOnInit() {
    this.getCategories();
  }

  public getCategories(): void {
    this.configurationData = [];
    this.configService.getCategories().
      subscribe(
      data => {
        this.configurationData = data.categories;
        console.log("This is the congfigurationData ", this.configurationData);
        this.configurationData.forEach(element => {
          this.getCategory(element.key);
        });

      },
      error => { console.log("error", error) });
  }

  private getCategory(category_name: string): void {
    var categoryValues = [];
    this.configService.getCategory(category_name).
      subscribe(
      data => {
        categoryValues.push(data);
        this.categoryData.push({ key: category_name, value: categoryValues })
        console.log("This is the categoryData ", this.categoryData);
      },
      error => { console.log("error", error) });
  }


  /**
   * 
   * @param category_name 
   * @param config_item 
   */
  private deletConfigItem(category_name, config_item) {
    console.log("category_name: ", category_name, " Config name: ", config_item);
    this.configService.deleteConfigItem(category_name, config_item).
      subscribe(
      data => {
        console.log("data ", data.value)
        if (data.value == "") {
          // Hot fix to bind DOM element with latest updated values
          location.reload();
        }
      },
      error => { console.log("error", error) });
  }
}
