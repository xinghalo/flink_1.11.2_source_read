/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzAddOnModule } from '../core/addon/addon.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { PlatformModule } from '@angular/cdk/platform';
import { NzAutosizeDirective } from './nz-autosize.directive';
import { NzInputGroupComponent } from './nz-input-group.component';
import { NzInputDirective } from './nz-input.directive';
var NzInputModule = /** @class */ (function () {
    function NzInputModule() {
    }
    NzInputModule.decorators = [
        { type: NgModule, args: [{
                    declarations: [NzInputDirective, NzInputGroupComponent, NzAutosizeDirective],
                    exports: [NzInputDirective, NzInputGroupComponent, NzAutosizeDirective],
                    imports: [CommonModule, FormsModule, NzIconModule, PlatformModule, NzAddOnModule]
                },] }
    ];
    return NzInputModule;
}());
export { NzInputModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotaW5wdXQubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImlucHV0L256LWlucHV0Lm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLGlCQUFpQixDQUFDO0FBQy9DLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxlQUFlLENBQUM7QUFDekMsT0FBTyxFQUFFLFdBQVcsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQzdDLE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQztBQUUzRCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFFdEQsT0FBTyxFQUFFLGNBQWMsRUFBRSxNQUFNLHVCQUF1QixDQUFDO0FBQ3ZELE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLHlCQUF5QixDQUFDO0FBQzlELE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLDRCQUE0QixDQUFDO0FBQ25FLE9BQU8sRUFBRSxnQkFBZ0IsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBRXhEO0lBQUE7SUFLNEIsQ0FBQzs7Z0JBTDVCLFFBQVEsU0FBQztvQkFDUixZQUFZLEVBQUUsQ0FBRSxnQkFBZ0IsRUFBRSxxQkFBcUIsRUFBRSxtQkFBbUIsQ0FBRTtvQkFDOUUsT0FBTyxFQUFPLENBQUUsZ0JBQWdCLEVBQUUscUJBQXFCLEVBQUUsbUJBQW1CLENBQUU7b0JBQzlFLE9BQU8sRUFBTyxDQUFFLFlBQVksRUFBRSxXQUFXLEVBQUUsWUFBWSxFQUFFLGNBQWMsRUFBRSxhQUFhLENBQUU7aUJBQ3pGOztJQUMyQixvQkFBQztDQUFBLEFBTDdCLElBSzZCO1NBQWhCLGFBQWEiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IEZvcm1zTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuaW1wb3J0IHsgTnpBZGRPbk1vZHVsZSB9IGZyb20gJy4uL2NvcmUvYWRkb24vYWRkb24ubW9kdWxlJztcblxuaW1wb3J0IHsgTnpJY29uTW9kdWxlIH0gZnJvbSAnLi4vaWNvbi9uei1pY29uLm1vZHVsZSc7XG5cbmltcG9ydCB7IFBsYXRmb3JtTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL3BsYXRmb3JtJztcbmltcG9ydCB7IE56QXV0b3NpemVEaXJlY3RpdmUgfSBmcm9tICcuL256LWF1dG9zaXplLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBOeklucHV0R3JvdXBDb21wb25lbnQgfSBmcm9tICcuL256LWlucHV0LWdyb3VwLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOeklucHV0RGlyZWN0aXZlIH0gZnJvbSAnLi9uei1pbnB1dC5kaXJlY3RpdmUnO1xuXG5ATmdNb2R1bGUoe1xuICBkZWNsYXJhdGlvbnM6IFsgTnpJbnB1dERpcmVjdGl2ZSwgTnpJbnB1dEdyb3VwQ29tcG9uZW50LCBOekF1dG9zaXplRGlyZWN0aXZlIF0sXG4gIGV4cG9ydHMgICAgIDogWyBOeklucHV0RGlyZWN0aXZlLCBOeklucHV0R3JvdXBDb21wb25lbnQsIE56QXV0b3NpemVEaXJlY3RpdmUgXSxcbiAgaW1wb3J0cyAgICAgOiBbIENvbW1vbk1vZHVsZSwgRm9ybXNNb2R1bGUsIE56SWNvbk1vZHVsZSwgUGxhdGZvcm1Nb2R1bGUsIE56QWRkT25Nb2R1bGUgXVxufSlcbmV4cG9ydCBjbGFzcyBOeklucHV0TW9kdWxlIHt9XG4iXX0=