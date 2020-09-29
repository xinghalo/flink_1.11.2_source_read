/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { LayoutModule } from '@angular/cdk/layout';
import { PlatformModule } from '@angular/cdk/platform';
import { NzGridModule } from '../grid/nz-grid.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzFormControlComponent } from './nz-form-control.component';
import { NzFormExplainComponent } from './nz-form-explain.component';
import { NzFormExtraComponent } from './nz-form-extra.component';
import { NzFormItemComponent } from './nz-form-item.component';
import { NzFormLabelComponent } from './nz-form-label.component';
import { NzFormSplitComponent } from './nz-form-split.component';
import { NzFormTextComponent } from './nz-form-text.component';
import { NzFormDirective } from './nz-form.directive';
export class NzFormModule {
}
NzFormModule.decorators = [
    { type: NgModule, args: [{
                declarations: [
                    NzFormExtraComponent,
                    NzFormLabelComponent,
                    NzFormDirective,
                    NzFormItemComponent,
                    NzFormControlComponent,
                    NzFormExplainComponent,
                    NzFormTextComponent,
                    NzFormSplitComponent
                ],
                exports: [
                    NzFormExtraComponent,
                    NzFormLabelComponent,
                    NzFormDirective,
                    NzFormItemComponent,
                    NzFormControlComponent,
                    NzFormExplainComponent,
                    NzFormTextComponent,
                    NzFormSplitComponent
                ],
                imports: [CommonModule, NzGridModule, NzIconModule, LayoutModule, PlatformModule]
            },] }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotZm9ybS5tb2R1bGUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiZm9ybS9uei1mb3JtLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQy9DLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFFekMsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHFCQUFxQixDQUFDO0FBQ25ELE9BQU8sRUFBRSxjQUFjLEVBQUUsTUFBTSx1QkFBdUIsQ0FBQztBQUN2RCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDdEQsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ3RELE9BQU8sRUFBRSxzQkFBc0IsRUFBRSxNQUFNLDZCQUE2QixDQUFDO0FBQ3JFLE9BQU8sRUFBRSxzQkFBc0IsRUFBRSxNQUFNLDZCQUE2QixDQUFDO0FBQ3JFLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLDJCQUEyQixDQUFDO0FBQ2pFLE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBQy9ELE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLDJCQUEyQixDQUFDO0FBQ2pFLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLDJCQUEyQixDQUFDO0FBQ2pFLE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBQy9ELE9BQU8sRUFBRSxlQUFlLEVBQUUsTUFBTSxxQkFBcUIsQ0FBQztBQXlCdEQsTUFBTSxPQUFPLFlBQVk7OztZQXZCeEIsUUFBUSxTQUFDO2dCQUNSLFlBQVksRUFBRTtvQkFDWixvQkFBb0I7b0JBQ3BCLG9CQUFvQjtvQkFDcEIsZUFBZTtvQkFDZixtQkFBbUI7b0JBQ25CLHNCQUFzQjtvQkFDdEIsc0JBQXNCO29CQUN0QixtQkFBbUI7b0JBQ25CLG9CQUFvQjtpQkFDckI7Z0JBQ0QsT0FBTyxFQUFFO29CQUNQLG9CQUFvQjtvQkFDcEIsb0JBQW9CO29CQUNwQixlQUFlO29CQUNmLG1CQUFtQjtvQkFDbkIsc0JBQXNCO29CQUN0QixzQkFBc0I7b0JBQ3RCLG1CQUFtQjtvQkFDbkIsb0JBQW9CO2lCQUNyQjtnQkFDRCxPQUFPLEVBQUUsQ0FBQyxZQUFZLEVBQUUsWUFBWSxFQUFFLFlBQVksRUFBRSxZQUFZLEVBQUUsY0FBYyxDQUFDO2FBQ2xGIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgQ29tbW9uTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7IE5nTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IExheW91dE1vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9sYXlvdXQnO1xuaW1wb3J0IHsgUGxhdGZvcm1Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jZGsvcGxhdGZvcm0nO1xuaW1wb3J0IHsgTnpHcmlkTW9kdWxlIH0gZnJvbSAnLi4vZ3JpZC9uei1ncmlkLm1vZHVsZSc7XG5pbXBvcnQgeyBOekljb25Nb2R1bGUgfSBmcm9tICcuLi9pY29uL256LWljb24ubW9kdWxlJztcbmltcG9ydCB7IE56Rm9ybUNvbnRyb2xDb21wb25lbnQgfSBmcm9tICcuL256LWZvcm0tY29udHJvbC5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpGb3JtRXhwbGFpbkNvbXBvbmVudCB9IGZyb20gJy4vbnotZm9ybS1leHBsYWluLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOekZvcm1FeHRyYUNvbXBvbmVudCB9IGZyb20gJy4vbnotZm9ybS1leHRyYS5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpGb3JtSXRlbUNvbXBvbmVudCB9IGZyb20gJy4vbnotZm9ybS1pdGVtLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOekZvcm1MYWJlbENvbXBvbmVudCB9IGZyb20gJy4vbnotZm9ybS1sYWJlbC5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpGb3JtU3BsaXRDb21wb25lbnQgfSBmcm9tICcuL256LWZvcm0tc3BsaXQuY29tcG9uZW50JztcbmltcG9ydCB7IE56Rm9ybVRleHRDb21wb25lbnQgfSBmcm9tICcuL256LWZvcm0tdGV4dC5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpGb3JtRGlyZWN0aXZlIH0gZnJvbSAnLi9uei1mb3JtLmRpcmVjdGl2ZSc7XG5cbkBOZ01vZHVsZSh7XG4gIGRlY2xhcmF0aW9uczogW1xuICAgIE56Rm9ybUV4dHJhQ29tcG9uZW50LFxuICAgIE56Rm9ybUxhYmVsQ29tcG9uZW50LFxuICAgIE56Rm9ybURpcmVjdGl2ZSxcbiAgICBOekZvcm1JdGVtQ29tcG9uZW50LFxuICAgIE56Rm9ybUNvbnRyb2xDb21wb25lbnQsXG4gICAgTnpGb3JtRXhwbGFpbkNvbXBvbmVudCxcbiAgICBOekZvcm1UZXh0Q29tcG9uZW50LFxuICAgIE56Rm9ybVNwbGl0Q29tcG9uZW50XG4gIF0sXG4gIGV4cG9ydHM6IFtcbiAgICBOekZvcm1FeHRyYUNvbXBvbmVudCxcbiAgICBOekZvcm1MYWJlbENvbXBvbmVudCxcbiAgICBOekZvcm1EaXJlY3RpdmUsXG4gICAgTnpGb3JtSXRlbUNvbXBvbmVudCxcbiAgICBOekZvcm1Db250cm9sQ29tcG9uZW50LFxuICAgIE56Rm9ybUV4cGxhaW5Db21wb25lbnQsXG4gICAgTnpGb3JtVGV4dENvbXBvbmVudCxcbiAgICBOekZvcm1TcGxpdENvbXBvbmVudFxuICBdLFxuICBpbXBvcnRzOiBbQ29tbW9uTW9kdWxlLCBOekdyaWRNb2R1bGUsIE56SWNvbk1vZHVsZSwgTGF5b3V0TW9kdWxlLCBQbGF0Zm9ybU1vZHVsZV1cbn0pXG5leHBvcnQgY2xhc3MgTnpGb3JtTW9kdWxlIHt9XG4iXX0=