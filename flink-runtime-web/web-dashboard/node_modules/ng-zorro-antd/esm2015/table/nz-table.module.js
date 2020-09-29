/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ScrollingModule } from '@angular/cdk/scrolling';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzCheckboxModule } from '../checkbox/nz-checkbox.module';
import { NzAddOnModule } from '../core/addon/addon.module';
import { NzDropDownModule } from '../dropdown/nz-dropdown.module';
import { NzEmptyModule } from '../empty/nz-empty.module';
import { NzI18nModule } from '../i18n/nz-i18n.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzMenuModule } from '../menu/nz-menu.module';
import { NzPaginationModule } from '../pagination/nz-pagination.module';
import { NzRadioModule } from '../radio/nz-radio.module';
import { NzSpinModule } from '../spin/nz-spin.module';
import { NzTableComponent } from './nz-table.component';
import { NzTbodyDirective } from './nz-tbody.directive';
import { NzTdComponent } from './nz-td.component';
import { NzThComponent } from './nz-th.component';
import { NzTheadComponent } from './nz-thead.component';
import { NzTrDirective } from './nz-tr.directive';
import { NzVirtualScrollDirective } from './nz-virtual-scroll.directive';
export class NzTableModule {
}
NzTableModule.decorators = [
    { type: NgModule, args: [{
                declarations: [
                    NzTableComponent,
                    NzThComponent,
                    NzTdComponent,
                    NzTheadComponent,
                    NzTbodyDirective,
                    NzTrDirective,
                    NzVirtualScrollDirective
                ],
                exports: [
                    NzTableComponent,
                    NzThComponent,
                    NzTdComponent,
                    NzTheadComponent,
                    NzTbodyDirective,
                    NzTrDirective,
                    NzVirtualScrollDirective
                ],
                imports: [
                    NzMenuModule,
                    FormsModule,
                    NzAddOnModule,
                    NzRadioModule,
                    NzCheckboxModule,
                    NzDropDownModule,
                    CommonModule,
                    NzPaginationModule,
                    NzSpinModule,
                    NzI18nModule,
                    NzIconModule,
                    NzEmptyModule,
                    ScrollingModule
                ]
            },] }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGFibGUubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRhYmxlL256LXRhYmxlLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ3pELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ3pDLE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUM3QyxPQUFPLEVBQUUsZ0JBQWdCLEVBQUUsTUFBTSxnQ0FBZ0MsQ0FBQztBQUNsRSxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sNEJBQTRCLENBQUM7QUFDM0QsT0FBTyxFQUFFLGdCQUFnQixFQUFFLE1BQU0sZ0NBQWdDLENBQUM7QUFDbEUsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBQ3pELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN0RCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDdEQsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ3RELE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLG9DQUFvQyxDQUFDO0FBQ3hFLE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSwwQkFBMEIsQ0FBQztBQUN6RCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFFdEQsT0FBTyxFQUFFLGdCQUFnQixFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDeEQsT0FBTyxFQUFFLGdCQUFnQixFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDeEQsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLG1CQUFtQixDQUFDO0FBQ2xELE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUNsRCxPQUFPLEVBQUUsZ0JBQWdCLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUN4RCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sbUJBQW1CLENBQUM7QUFDbEQsT0FBTyxFQUFFLHdCQUF3QixFQUFFLE1BQU0sK0JBQStCLENBQUM7QUFxQ3pFLE1BQU0sT0FBTyxhQUFhOzs7WUFuQ3pCLFFBQVEsU0FBQztnQkFDUixZQUFZLEVBQUU7b0JBQ1osZ0JBQWdCO29CQUNoQixhQUFhO29CQUNiLGFBQWE7b0JBQ2IsZ0JBQWdCO29CQUNoQixnQkFBZ0I7b0JBQ2hCLGFBQWE7b0JBQ2Isd0JBQXdCO2lCQUN6QjtnQkFDRCxPQUFPLEVBQUU7b0JBQ1AsZ0JBQWdCO29CQUNoQixhQUFhO29CQUNiLGFBQWE7b0JBQ2IsZ0JBQWdCO29CQUNoQixnQkFBZ0I7b0JBQ2hCLGFBQWE7b0JBQ2Isd0JBQXdCO2lCQUN6QjtnQkFDRCxPQUFPLEVBQUU7b0JBQ1AsWUFBWTtvQkFDWixXQUFXO29CQUNYLGFBQWE7b0JBQ2IsYUFBYTtvQkFDYixnQkFBZ0I7b0JBQ2hCLGdCQUFnQjtvQkFDaEIsWUFBWTtvQkFDWixrQkFBa0I7b0JBQ2xCLFlBQVk7b0JBQ1osWUFBWTtvQkFDWixZQUFZO29CQUNaLGFBQWE7b0JBQ2IsZUFBZTtpQkFDaEI7YUFDRiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IFNjcm9sbGluZ01vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9zY3JvbGxpbmcnO1xuaW1wb3J0IHsgQ29tbW9uTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7IE5nTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBGb3Jtc01vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2Zvcm1zJztcbmltcG9ydCB7IE56Q2hlY2tib3hNb2R1bGUgfSBmcm9tICcuLi9jaGVja2JveC9uei1jaGVja2JveC5tb2R1bGUnO1xuaW1wb3J0IHsgTnpBZGRPbk1vZHVsZSB9IGZyb20gJy4uL2NvcmUvYWRkb24vYWRkb24ubW9kdWxlJztcbmltcG9ydCB7IE56RHJvcERvd25Nb2R1bGUgfSBmcm9tICcuLi9kcm9wZG93bi9uei1kcm9wZG93bi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpFbXB0eU1vZHVsZSB9IGZyb20gJy4uL2VtcHR5L256LWVtcHR5Lm1vZHVsZSc7XG5pbXBvcnQgeyBOekkxOG5Nb2R1bGUgfSBmcm9tICcuLi9pMThuL256LWkxOG4ubW9kdWxlJztcbmltcG9ydCB7IE56SWNvbk1vZHVsZSB9IGZyb20gJy4uL2ljb24vbnotaWNvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpNZW51TW9kdWxlIH0gZnJvbSAnLi4vbWVudS9uei1tZW51Lm1vZHVsZSc7XG5pbXBvcnQgeyBOelBhZ2luYXRpb25Nb2R1bGUgfSBmcm9tICcuLi9wYWdpbmF0aW9uL256LXBhZ2luYXRpb24ubW9kdWxlJztcbmltcG9ydCB7IE56UmFkaW9Nb2R1bGUgfSBmcm9tICcuLi9yYWRpby9uei1yYWRpby5tb2R1bGUnO1xuaW1wb3J0IHsgTnpTcGluTW9kdWxlIH0gZnJvbSAnLi4vc3Bpbi9uei1zcGluLm1vZHVsZSc7XG5cbmltcG9ydCB7IE56VGFibGVDb21wb25lbnQgfSBmcm9tICcuL256LXRhYmxlLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOelRib2R5RGlyZWN0aXZlIH0gZnJvbSAnLi9uei10Ym9keS5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgTnpUZENvbXBvbmVudCB9IGZyb20gJy4vbnotdGQuY29tcG9uZW50JztcbmltcG9ydCB7IE56VGhDb21wb25lbnQgfSBmcm9tICcuL256LXRoLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOelRoZWFkQ29tcG9uZW50IH0gZnJvbSAnLi9uei10aGVhZC5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpUckRpcmVjdGl2ZSB9IGZyb20gJy4vbnotdHIuZGlyZWN0aXZlJztcbmltcG9ydCB7IE56VmlydHVhbFNjcm9sbERpcmVjdGl2ZSB9IGZyb20gJy4vbnotdmlydHVhbC1zY3JvbGwuZGlyZWN0aXZlJztcblxuQE5nTW9kdWxlKHtcbiAgZGVjbGFyYXRpb25zOiBbXG4gICAgTnpUYWJsZUNvbXBvbmVudCxcbiAgICBOelRoQ29tcG9uZW50LFxuICAgIE56VGRDb21wb25lbnQsXG4gICAgTnpUaGVhZENvbXBvbmVudCxcbiAgICBOelRib2R5RGlyZWN0aXZlLFxuICAgIE56VHJEaXJlY3RpdmUsXG4gICAgTnpWaXJ0dWFsU2Nyb2xsRGlyZWN0aXZlXG4gIF0sXG4gIGV4cG9ydHM6IFtcbiAgICBOelRhYmxlQ29tcG9uZW50LFxuICAgIE56VGhDb21wb25lbnQsXG4gICAgTnpUZENvbXBvbmVudCxcbiAgICBOelRoZWFkQ29tcG9uZW50LFxuICAgIE56VGJvZHlEaXJlY3RpdmUsXG4gICAgTnpUckRpcmVjdGl2ZSxcbiAgICBOelZpcnR1YWxTY3JvbGxEaXJlY3RpdmVcbiAgXSxcbiAgaW1wb3J0czogW1xuICAgIE56TWVudU1vZHVsZSxcbiAgICBGb3Jtc01vZHVsZSxcbiAgICBOekFkZE9uTW9kdWxlLFxuICAgIE56UmFkaW9Nb2R1bGUsXG4gICAgTnpDaGVja2JveE1vZHVsZSxcbiAgICBOekRyb3BEb3duTW9kdWxlLFxuICAgIENvbW1vbk1vZHVsZSxcbiAgICBOelBhZ2luYXRpb25Nb2R1bGUsXG4gICAgTnpTcGluTW9kdWxlLFxuICAgIE56STE4bk1vZHVsZSxcbiAgICBOekljb25Nb2R1bGUsXG4gICAgTnpFbXB0eU1vZHVsZSxcbiAgICBTY3JvbGxpbmdNb2R1bGVcbiAgXVxufSlcbmV4cG9ydCBjbGFzcyBOelRhYmxlTW9kdWxlIHt9XG4iXX0=