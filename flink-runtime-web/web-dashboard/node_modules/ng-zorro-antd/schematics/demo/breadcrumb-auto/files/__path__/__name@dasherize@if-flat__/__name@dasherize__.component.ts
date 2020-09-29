import { Component } from '@angular/core';

@Component({
  selector: '<%= selector %>',
  <% if(inlineTemplate) { %>template: `
    <nz-breadcrumb [nzAutoGenerate]="true">
      Please refer to StackBlitz demo.
    </nz-breadcrumb>
  `<% } else { %>templateUrl: './<%= dasherize(name) %>.component.html'<% } %>
})
export class <%= classify(name) %>Component {}
