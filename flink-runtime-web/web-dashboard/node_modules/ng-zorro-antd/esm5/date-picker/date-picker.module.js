/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { NzOverlayModule } from '../core/overlay/nz-overlay.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { LibPackerModule } from './lib/lib-packer.module';
import { NzDatePickerComponent } from './date-picker.component';
import { DateRangePickerComponent } from './date-range-picker.component';
import { HeaderPickerComponent } from './header-picker.component';
import { NzMonthPickerComponent } from './month-picker.component';
import { NzPickerComponent } from './picker.component';
import { NzRangePickerComponent } from './range-picker.component';
import { NzWeekPickerComponent } from './week-picker.component';
import { NzYearPickerComponent } from './year-picker.component';
var NzDatePickerModule = /** @class */ (function () {
    function NzDatePickerModule() {
    }
    NzDatePickerModule.decorators = [
        { type: NgModule, args: [{
                    imports: [CommonModule, OverlayModule, LibPackerModule, NzIconModule, NzOverlayModule, NzNoAnimationModule],
                    exports: [
                        NzDatePickerComponent,
                        NzRangePickerComponent,
                        NzMonthPickerComponent,
                        NzYearPickerComponent,
                        NzWeekPickerComponent
                    ],
                    declarations: [
                        HeaderPickerComponent,
                        DateRangePickerComponent,
                        NzPickerComponent,
                        NzDatePickerComponent,
                        NzMonthPickerComponent,
                        NzYearPickerComponent,
                        NzWeekPickerComponent,
                        NzRangePickerComponent
                    ]
                },] }
    ];
    return NzDatePickerModule;
}());
export { NzDatePickerModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZGF0ZS1waWNrZXIubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImRhdGUtcGlja2VyL2RhdGUtcGlja2VyLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRXpDLE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLDZDQUE2QyxDQUFDO0FBQ2xGLE9BQU8sRUFBRSxlQUFlLEVBQUUsTUFBTSxtQ0FBbUMsQ0FBQztBQUNwRSxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFFdEQsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBRTFELE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBQ2hFLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLCtCQUErQixDQUFDO0FBQ3pFLE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLDJCQUEyQixDQUFDO0FBQ2xFLE9BQU8sRUFBRSxzQkFBc0IsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBQ2xFLE9BQU8sRUFBRSxpQkFBaUIsRUFBRSxNQUFNLG9CQUFvQixDQUFDO0FBQ3ZELE9BQU8sRUFBRSxzQkFBc0IsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBQ2xFLE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBQ2hFLE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBRWhFO0lBQUE7SUFxQmlDLENBQUM7O2dCQXJCakMsUUFBUSxTQUFDO29CQUNSLE9BQU8sRUFBRSxDQUFDLFlBQVksRUFBRSxhQUFhLEVBQUUsZUFBZSxFQUFFLFlBQVksRUFBRSxlQUFlLEVBQUUsbUJBQW1CLENBQUM7b0JBQzNHLE9BQU8sRUFBRTt3QkFDUCxxQkFBcUI7d0JBQ3JCLHNCQUFzQjt3QkFDdEIsc0JBQXNCO3dCQUN0QixxQkFBcUI7d0JBQ3JCLHFCQUFxQjtxQkFDdEI7b0JBQ0QsWUFBWSxFQUFFO3dCQUNaLHFCQUFxQjt3QkFDckIsd0JBQXdCO3dCQUN4QixpQkFBaUI7d0JBRWpCLHFCQUFxQjt3QkFDckIsc0JBQXNCO3dCQUN0QixxQkFBcUI7d0JBQ3JCLHFCQUFxQjt3QkFDckIsc0JBQXNCO3FCQUN2QjtpQkFDRjs7SUFDZ0MseUJBQUM7Q0FBQSxBQXJCbEMsSUFxQmtDO1NBQXJCLGtCQUFrQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IE92ZXJsYXlNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jZGsvb3ZlcmxheSc7XG5pbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgTnpOb0FuaW1hdGlvbk1vZHVsZSB9IGZyb20gJy4uL2NvcmUvbm8tYW5pbWF0aW9uL256LW5vLWFuaW1hdGlvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpPdmVybGF5TW9kdWxlIH0gZnJvbSAnLi4vY29yZS9vdmVybGF5L256LW92ZXJsYXkubW9kdWxlJztcbmltcG9ydCB7IE56SWNvbk1vZHVsZSB9IGZyb20gJy4uL2ljb24vbnotaWNvbi5tb2R1bGUnO1xuXG5pbXBvcnQgeyBMaWJQYWNrZXJNb2R1bGUgfSBmcm9tICcuL2xpYi9saWItcGFja2VyLm1vZHVsZSc7XG5cbmltcG9ydCB7IE56RGF0ZVBpY2tlckNvbXBvbmVudCB9IGZyb20gJy4vZGF0ZS1waWNrZXIuY29tcG9uZW50JztcbmltcG9ydCB7IERhdGVSYW5nZVBpY2tlckNvbXBvbmVudCB9IGZyb20gJy4vZGF0ZS1yYW5nZS1waWNrZXIuY29tcG9uZW50JztcbmltcG9ydCB7IEhlYWRlclBpY2tlckNvbXBvbmVudCB9IGZyb20gJy4vaGVhZGVyLXBpY2tlci5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpNb250aFBpY2tlckNvbXBvbmVudCB9IGZyb20gJy4vbW9udGgtcGlja2VyLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOelBpY2tlckNvbXBvbmVudCB9IGZyb20gJy4vcGlja2VyLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOelJhbmdlUGlja2VyQ29tcG9uZW50IH0gZnJvbSAnLi9yYW5nZS1waWNrZXIuY29tcG9uZW50JztcbmltcG9ydCB7IE56V2Vla1BpY2tlckNvbXBvbmVudCB9IGZyb20gJy4vd2Vlay1waWNrZXIuY29tcG9uZW50JztcbmltcG9ydCB7IE56WWVhclBpY2tlckNvbXBvbmVudCB9IGZyb20gJy4veWVhci1waWNrZXIuY29tcG9uZW50JztcblxuQE5nTW9kdWxlKHtcbiAgaW1wb3J0czogW0NvbW1vbk1vZHVsZSwgT3ZlcmxheU1vZHVsZSwgTGliUGFja2VyTW9kdWxlLCBOekljb25Nb2R1bGUsIE56T3ZlcmxheU1vZHVsZSwgTnpOb0FuaW1hdGlvbk1vZHVsZV0sXG4gIGV4cG9ydHM6IFtcbiAgICBOekRhdGVQaWNrZXJDb21wb25lbnQsXG4gICAgTnpSYW5nZVBpY2tlckNvbXBvbmVudCxcbiAgICBOek1vbnRoUGlja2VyQ29tcG9uZW50LFxuICAgIE56WWVhclBpY2tlckNvbXBvbmVudCxcbiAgICBOeldlZWtQaWNrZXJDb21wb25lbnRcbiAgXSxcbiAgZGVjbGFyYXRpb25zOiBbXG4gICAgSGVhZGVyUGlja2VyQ29tcG9uZW50LFxuICAgIERhdGVSYW5nZVBpY2tlckNvbXBvbmVudCxcbiAgICBOelBpY2tlckNvbXBvbmVudCxcblxuICAgIE56RGF0ZVBpY2tlckNvbXBvbmVudCxcbiAgICBOek1vbnRoUGlja2VyQ29tcG9uZW50LFxuICAgIE56WWVhclBpY2tlckNvbXBvbmVudCxcbiAgICBOeldlZWtQaWNrZXJDb21wb25lbnQsXG4gICAgTnpSYW5nZVBpY2tlckNvbXBvbmVudFxuICBdXG59KVxuZXhwb3J0IGNsYXNzIE56RGF0ZVBpY2tlck1vZHVsZSB7fVxuIl19