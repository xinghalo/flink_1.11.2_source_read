/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzButtonModule } from '../button/nz-button.module';
import { NzCheckboxModule } from '../checkbox/nz-checkbox.module';
import { NzI18nModule } from '../i18n/nz-i18n.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzInputModule } from '../input/nz-input.module';
import { NzEmptyModule } from '../empty/nz-empty.module';
import { NzTransferListComponent } from './nz-transfer-list.component';
import { NzTransferSearchComponent } from './nz-transfer-search.component';
import { NzTransferComponent } from './nz-transfer.component';
export class NzTransferModule {
}
NzTransferModule.decorators = [
    { type: NgModule, args: [{
                imports: [
                    CommonModule,
                    FormsModule,
                    NzCheckboxModule,
                    NzButtonModule,
                    NzInputModule,
                    NzI18nModule,
                    NzIconModule,
                    NzEmptyModule
                ],
                declarations: [NzTransferComponent, NzTransferListComponent, NzTransferSearchComponent],
                exports: [NzTransferComponent]
            },] }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdHJhbnNmZXIubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRyYW5zZmVyL256LXRyYW5zZmVyLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQy9DLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFDekMsT0FBTyxFQUFFLFdBQVcsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBRTdDLE9BQU8sRUFBRSxjQUFjLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQztBQUM1RCxPQUFPLEVBQUUsZ0JBQWdCLEVBQUUsTUFBTSxnQ0FBZ0MsQ0FBQztBQUNsRSxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDdEQsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHdCQUF3QixDQUFDO0FBQ3RELE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSwwQkFBMEIsQ0FBQztBQUV6RCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDekQsT0FBTyxFQUFFLHVCQUF1QixFQUFFLE1BQU0sOEJBQThCLENBQUM7QUFDdkUsT0FBTyxFQUFFLHlCQUF5QixFQUFFLE1BQU0sZ0NBQWdDLENBQUM7QUFDM0UsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0seUJBQXlCLENBQUM7QUFnQjlELE1BQU0sT0FBTyxnQkFBZ0I7OztZQWQ1QixRQUFRLFNBQUM7Z0JBQ1IsT0FBTyxFQUFFO29CQUNQLFlBQVk7b0JBQ1osV0FBVztvQkFDWCxnQkFBZ0I7b0JBQ2hCLGNBQWM7b0JBQ2QsYUFBYTtvQkFDYixZQUFZO29CQUNaLFlBQVk7b0JBQ1osYUFBYTtpQkFDZDtnQkFDRCxZQUFZLEVBQUUsQ0FBQyxtQkFBbUIsRUFBRSx1QkFBdUIsRUFBRSx5QkFBeUIsQ0FBQztnQkFDdkYsT0FBTyxFQUFFLENBQUMsbUJBQW1CLENBQUM7YUFDL0IiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IEZvcm1zTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuXG5pbXBvcnQgeyBOekJ1dHRvbk1vZHVsZSB9IGZyb20gJy4uL2J1dHRvbi9uei1idXR0b24ubW9kdWxlJztcbmltcG9ydCB7IE56Q2hlY2tib3hNb2R1bGUgfSBmcm9tICcuLi9jaGVja2JveC9uei1jaGVja2JveC5tb2R1bGUnO1xuaW1wb3J0IHsgTnpJMThuTW9kdWxlIH0gZnJvbSAnLi4vaTE4bi9uei1pMThuLm1vZHVsZSc7XG5pbXBvcnQgeyBOekljb25Nb2R1bGUgfSBmcm9tICcuLi9pY29uL256LWljb24ubW9kdWxlJztcbmltcG9ydCB7IE56SW5wdXRNb2R1bGUgfSBmcm9tICcuLi9pbnB1dC9uei1pbnB1dC5tb2R1bGUnO1xuXG5pbXBvcnQgeyBOekVtcHR5TW9kdWxlIH0gZnJvbSAnLi4vZW1wdHkvbnotZW1wdHkubW9kdWxlJztcbmltcG9ydCB7IE56VHJhbnNmZXJMaXN0Q29tcG9uZW50IH0gZnJvbSAnLi9uei10cmFuc2Zlci1saXN0LmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOelRyYW5zZmVyU2VhcmNoQ29tcG9uZW50IH0gZnJvbSAnLi9uei10cmFuc2Zlci1zZWFyY2guY29tcG9uZW50JztcbmltcG9ydCB7IE56VHJhbnNmZXJDb21wb25lbnQgfSBmcm9tICcuL256LXRyYW5zZmVyLmNvbXBvbmVudCc7XG5cbkBOZ01vZHVsZSh7XG4gIGltcG9ydHM6IFtcbiAgICBDb21tb25Nb2R1bGUsXG4gICAgRm9ybXNNb2R1bGUsXG4gICAgTnpDaGVja2JveE1vZHVsZSxcbiAgICBOekJ1dHRvbk1vZHVsZSxcbiAgICBOeklucHV0TW9kdWxlLFxuICAgIE56STE4bk1vZHVsZSxcbiAgICBOekljb25Nb2R1bGUsXG4gICAgTnpFbXB0eU1vZHVsZVxuICBdLFxuICBkZWNsYXJhdGlvbnM6IFtOelRyYW5zZmVyQ29tcG9uZW50LCBOelRyYW5zZmVyTGlzdENvbXBvbmVudCwgTnpUcmFuc2ZlclNlYXJjaENvbXBvbmVudF0sXG4gIGV4cG9ydHM6IFtOelRyYW5zZmVyQ29tcG9uZW50XVxufSlcbmV4cG9ydCBjbGFzcyBOelRyYW5zZmVyTW9kdWxlIHt9XG4iXX0=