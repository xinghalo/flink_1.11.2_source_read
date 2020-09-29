/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { NzOverlayModule } from '../core/overlay/nz-overlay.module';
import { NzEmptyModule } from '../empty/nz-empty.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzInputModule } from '../input/nz-input.module';
import { NzCascaderOptionComponent } from './nz-cascader-li.component';
import { NzCascaderComponent } from './nz-cascader.component';
export class NzCascaderModule {
}
NzCascaderModule.decorators = [
    { type: NgModule, args: [{
                imports: [
                    CommonModule,
                    FormsModule,
                    OverlayModule,
                    NzInputModule,
                    NzIconModule,
                    NzEmptyModule,
                    NzOverlayModule,
                    NzNoAnimationModule
                ],
                declarations: [NzCascaderComponent, NzCascaderOptionComponent],
                exports: [NzCascaderComponent]
            },] }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2FzY2FkZXIubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNhc2NhZGVyL256LWNhc2NhZGVyLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ3pDLE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUU3QyxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSw2Q0FBNkMsQ0FBQztBQUNsRixPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0sbUNBQW1DLENBQUM7QUFDcEUsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBQ3pELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN0RCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDekQsT0FBTyxFQUFFLHlCQUF5QixFQUFFLE1BQU0sNEJBQTRCLENBQUM7QUFDdkUsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0seUJBQXlCLENBQUM7QUFnQjlELE1BQU0sT0FBTyxnQkFBZ0I7OztZQWQ1QixRQUFRLFNBQUM7Z0JBQ1IsT0FBTyxFQUFFO29CQUNQLFlBQVk7b0JBQ1osV0FBVztvQkFDWCxhQUFhO29CQUNiLGFBQWE7b0JBQ2IsWUFBWTtvQkFDWixhQUFhO29CQUNiLGVBQWU7b0JBQ2YsbUJBQW1CO2lCQUNwQjtnQkFDRCxZQUFZLEVBQUUsQ0FBQyxtQkFBbUIsRUFBRSx5QkFBeUIsQ0FBQztnQkFDOUQsT0FBTyxFQUFFLENBQUMsbUJBQW1CLENBQUM7YUFDL0IiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBPdmVybGF5TW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL292ZXJsYXknO1xuaW1wb3J0IHsgQ29tbW9uTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7IE5nTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBGb3Jtc01vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2Zvcm1zJztcblxuaW1wb3J0IHsgTnpOb0FuaW1hdGlvbk1vZHVsZSB9IGZyb20gJy4uL2NvcmUvbm8tYW5pbWF0aW9uL256LW5vLWFuaW1hdGlvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpPdmVybGF5TW9kdWxlIH0gZnJvbSAnLi4vY29yZS9vdmVybGF5L256LW92ZXJsYXkubW9kdWxlJztcbmltcG9ydCB7IE56RW1wdHlNb2R1bGUgfSBmcm9tICcuLi9lbXB0eS9uei1lbXB0eS5tb2R1bGUnO1xuaW1wb3J0IHsgTnpJY29uTW9kdWxlIH0gZnJvbSAnLi4vaWNvbi9uei1pY29uLm1vZHVsZSc7XG5pbXBvcnQgeyBOeklucHV0TW9kdWxlIH0gZnJvbSAnLi4vaW5wdXQvbnotaW5wdXQubW9kdWxlJztcbmltcG9ydCB7IE56Q2FzY2FkZXJPcHRpb25Db21wb25lbnQgfSBmcm9tICcuL256LWNhc2NhZGVyLWxpLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOekNhc2NhZGVyQ29tcG9uZW50IH0gZnJvbSAnLi9uei1jYXNjYWRlci5jb21wb25lbnQnO1xuXG5ATmdNb2R1bGUoe1xuICBpbXBvcnRzOiBbXG4gICAgQ29tbW9uTW9kdWxlLFxuICAgIEZvcm1zTW9kdWxlLFxuICAgIE92ZXJsYXlNb2R1bGUsXG4gICAgTnpJbnB1dE1vZHVsZSxcbiAgICBOekljb25Nb2R1bGUsXG4gICAgTnpFbXB0eU1vZHVsZSxcbiAgICBOek92ZXJsYXlNb2R1bGUsXG4gICAgTnpOb0FuaW1hdGlvbk1vZHVsZVxuICBdLFxuICBkZWNsYXJhdGlvbnM6IFtOekNhc2NhZGVyQ29tcG9uZW50LCBOekNhc2NhZGVyT3B0aW9uQ29tcG9uZW50XSxcbiAgZXhwb3J0czogW056Q2FzY2FkZXJDb21wb25lbnRdXG59KVxuZXhwb3J0IGNsYXNzIE56Q2FzY2FkZXJNb2R1bGUge31cbiJdfQ==