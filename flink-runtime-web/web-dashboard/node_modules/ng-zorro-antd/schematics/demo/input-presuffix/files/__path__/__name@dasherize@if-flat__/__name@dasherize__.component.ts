import { Component } from '@angular/core';

@Component({
  selector: '<%= selector %>',
  <% if(inlineTemplate) { %>template: `
    <nz-input-group [nzSuffix]="suffixTemplate" [nzPrefix]="prefixTemplate">
      <input type="text" nz-input placeholder="Enter your username" [(ngModel)]="username" />
    </nz-input-group>
    <ng-template #prefixTemplate><i nz-icon type="user"></i></ng-template>
    <ng-template #suffixTemplate
      ><i nz-icon type="close-circle" (click)="username = ''" *ngIf="username"></i
    ></ng-template>
  `<% } else { %>templateUrl: './<%= dasherize(name) %>.component.html'<% } %>,
  <% if(inlineStyle) { %>styles: [`
      .anticon-close-circle {
        cursor: pointer;
        color: #ccc;
        transition: color 0.3s;
        font-size: 12px;
      }

      .anticon-close-circle:hover {
        color: #999;
      }

      .anticon-close-circle:active {
        color: #666;
      }
    `]<% } else { %>styleUrls: ['./<%= dasherize(name) %>.component.<%= styleext %>']<% } %>
})
export class <%= classify(name) %>Component {
  username: string;
}
