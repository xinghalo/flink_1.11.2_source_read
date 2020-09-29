/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzI18nModule } from '../i18n/nz-i18n.module';
import { NzRadioModule } from '../radio/nz-radio.module';
import { NzSelectModule } from '../select/nz-select.module';
import { NzDateCellDirective, NzDateFullCellDirective, NzMonthCellDirective, NzMonthFullCellDirective } from './nz-calendar-cells';
import { NzCalendarHeaderComponent } from './nz-calendar-header.component';
import { NzCalendarComponent } from './nz-calendar.component';
var NzCalendarModule = /** @class */ (function () {
    function NzCalendarModule() {
    }
    NzCalendarModule.decorators = [
        { type: NgModule, args: [{
                    declarations: [
                        NzCalendarHeaderComponent,
                        NzCalendarComponent,
                        NzDateCellDirective,
                        NzDateFullCellDirective,
                        NzMonthCellDirective,
                        NzMonthFullCellDirective
                    ],
                    exports: [
                        NzCalendarComponent,
                        NzDateCellDirective,
                        NzDateFullCellDirective,
                        NzMonthCellDirective,
                        NzMonthFullCellDirective
                    ],
                    imports: [CommonModule, FormsModule, NzI18nModule, NzRadioModule, NzSelectModule]
                },] }
    ];
    return NzCalendarModule;
}());
export { NzCalendarModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FsZW5kYXIubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNhbGVuZGFyL256LWNhbGVuZGFyLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQy9DLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFDekMsT0FBTyxFQUFFLFdBQVcsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQzdDLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN0RCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDekQsT0FBTyxFQUFFLGNBQWMsRUFBRSxNQUFNLDRCQUE0QixDQUFDO0FBQzVELE9BQU8sRUFDTCxtQkFBbUIsRUFDbkIsdUJBQXVCLEVBQ3ZCLG9CQUFvQixFQUNwQix3QkFBd0IsRUFDekIsTUFBTSxxQkFBcUIsQ0FBQztBQUM3QixPQUFPLEVBQUUseUJBQXlCLEVBQUUsTUFBTSxnQ0FBZ0MsQ0FBQztBQUMzRSxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSx5QkFBeUIsQ0FBQztBQUU5RDtJQUFBO0lBa0IrQixDQUFDOztnQkFsQi9CLFFBQVEsU0FBQztvQkFDUixZQUFZLEVBQUU7d0JBQ1oseUJBQXlCO3dCQUN6QixtQkFBbUI7d0JBQ25CLG1CQUFtQjt3QkFDbkIsdUJBQXVCO3dCQUN2QixvQkFBb0I7d0JBQ3BCLHdCQUF3QjtxQkFDekI7b0JBQ0QsT0FBTyxFQUFFO3dCQUNQLG1CQUFtQjt3QkFDbkIsbUJBQW1CO3dCQUNuQix1QkFBdUI7d0JBQ3ZCLG9CQUFvQjt3QkFDcEIsd0JBQXdCO3FCQUN6QjtvQkFDRCxPQUFPLEVBQUUsQ0FBQyxZQUFZLEVBQUUsV0FBVyxFQUFFLFlBQVksRUFBRSxhQUFhLEVBQUUsY0FBYyxDQUFDO2lCQUNsRjs7SUFDOEIsdUJBQUM7Q0FBQSxBQWxCaEMsSUFrQmdDO1NBQW5CLGdCQUFnQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IENvbW1vbk1vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQgeyBOZ01vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgRm9ybXNNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9mb3Jtcyc7XG5pbXBvcnQgeyBOekkxOG5Nb2R1bGUgfSBmcm9tICcuLi9pMThuL256LWkxOG4ubW9kdWxlJztcbmltcG9ydCB7IE56UmFkaW9Nb2R1bGUgfSBmcm9tICcuLi9yYWRpby9uei1yYWRpby5tb2R1bGUnO1xuaW1wb3J0IHsgTnpTZWxlY3RNb2R1bGUgfSBmcm9tICcuLi9zZWxlY3Qvbnotc2VsZWN0Lm1vZHVsZSc7XG5pbXBvcnQge1xuICBOekRhdGVDZWxsRGlyZWN0aXZlLFxuICBOekRhdGVGdWxsQ2VsbERpcmVjdGl2ZSxcbiAgTnpNb250aENlbGxEaXJlY3RpdmUsXG4gIE56TW9udGhGdWxsQ2VsbERpcmVjdGl2ZVxufSBmcm9tICcuL256LWNhbGVuZGFyLWNlbGxzJztcbmltcG9ydCB7IE56Q2FsZW5kYXJIZWFkZXJDb21wb25lbnQgfSBmcm9tICcuL256LWNhbGVuZGFyLWhlYWRlci5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpDYWxlbmRhckNvbXBvbmVudCB9IGZyb20gJy4vbnotY2FsZW5kYXIuY29tcG9uZW50JztcblxuQE5nTW9kdWxlKHtcbiAgZGVjbGFyYXRpb25zOiBbXG4gICAgTnpDYWxlbmRhckhlYWRlckNvbXBvbmVudCxcbiAgICBOekNhbGVuZGFyQ29tcG9uZW50LFxuICAgIE56RGF0ZUNlbGxEaXJlY3RpdmUsXG4gICAgTnpEYXRlRnVsbENlbGxEaXJlY3RpdmUsXG4gICAgTnpNb250aENlbGxEaXJlY3RpdmUsXG4gICAgTnpNb250aEZ1bGxDZWxsRGlyZWN0aXZlXG4gIF0sXG4gIGV4cG9ydHM6IFtcbiAgICBOekNhbGVuZGFyQ29tcG9uZW50LFxuICAgIE56RGF0ZUNlbGxEaXJlY3RpdmUsXG4gICAgTnpEYXRlRnVsbENlbGxEaXJlY3RpdmUsXG4gICAgTnpNb250aENlbGxEaXJlY3RpdmUsXG4gICAgTnpNb250aEZ1bGxDZWxsRGlyZWN0aXZlXG4gIF0sXG4gIGltcG9ydHM6IFtDb21tb25Nb2R1bGUsIEZvcm1zTW9kdWxlLCBOekkxOG5Nb2R1bGUsIE56UmFkaW9Nb2R1bGUsIE56U2VsZWN0TW9kdWxlXVxufSlcbmV4cG9ydCBjbGFzcyBOekNhbGVuZGFyTW9kdWxlIHt9XG4iXX0=