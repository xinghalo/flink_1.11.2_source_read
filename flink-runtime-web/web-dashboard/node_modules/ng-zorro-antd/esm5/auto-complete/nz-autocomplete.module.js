/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzAddOnModule } from '../core/addon/addon.module';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { NzAutocompleteOptgroupComponent } from './nz-autocomplete-optgroup.component';
import { NzAutocompleteOptionComponent } from './nz-autocomplete-option.component';
import { NzAutocompleteTriggerDirective } from './nz-autocomplete-trigger.directive';
import { NzAutocompleteComponent } from './nz-autocomplete.component';
var NzAutocompleteModule = /** @class */ (function () {
    function NzAutocompleteModule() {
    }
    NzAutocompleteModule.decorators = [
        { type: NgModule, args: [{
                    declarations: [
                        NzAutocompleteComponent,
                        NzAutocompleteOptionComponent,
                        NzAutocompleteTriggerDirective,
                        NzAutocompleteOptgroupComponent
                    ],
                    exports: [
                        NzAutocompleteComponent,
                        NzAutocompleteOptionComponent,
                        NzAutocompleteTriggerDirective,
                        NzAutocompleteOptgroupComponent
                    ],
                    imports: [CommonModule, OverlayModule, FormsModule, NzAddOnModule, NzNoAnimationModule]
                },] }
    ];
    return NzAutocompleteModule;
}());
export { NzAutocompleteModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYXV0b2NvbXBsZXRlLm1vZHVsZS5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJhdXRvLWNvbXBsZXRlL256LWF1dG9jb21wbGV0ZS5tb2R1bGUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNyRCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0saUJBQWlCLENBQUM7QUFDL0MsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLGVBQWUsQ0FBQztBQUN6QyxPQUFPLEVBQUUsV0FBVyxFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFFN0MsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLDRCQUE0QixDQUFDO0FBQzNELE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxNQUFNLDZDQUE2QyxDQUFDO0FBRWxGLE9BQU8sRUFBRSwrQkFBK0IsRUFBRSxNQUFNLHNDQUFzQyxDQUFDO0FBQ3ZGLE9BQU8sRUFBRSw2QkFBNkIsRUFBRSxNQUFNLG9DQUFvQyxDQUFDO0FBQ25GLE9BQU8sRUFBRSw4QkFBOEIsRUFBRSxNQUFNLHFDQUFxQyxDQUFDO0FBQ3JGLE9BQU8sRUFBRSx1QkFBdUIsRUFBRSxNQUFNLDZCQUE2QixDQUFDO0FBRXRFO0lBQUE7SUFlbUMsQ0FBQzs7Z0JBZm5DLFFBQVEsU0FBQztvQkFDUixZQUFZLEVBQUU7d0JBQ1osdUJBQXVCO3dCQUN2Qiw2QkFBNkI7d0JBQzdCLDhCQUE4Qjt3QkFDOUIsK0JBQStCO3FCQUNoQztvQkFDRCxPQUFPLEVBQUU7d0JBQ1AsdUJBQXVCO3dCQUN2Qiw2QkFBNkI7d0JBQzdCLDhCQUE4Qjt3QkFDOUIsK0JBQStCO3FCQUNoQztvQkFDRCxPQUFPLEVBQUUsQ0FBQyxZQUFZLEVBQUUsYUFBYSxFQUFFLFdBQVcsRUFBRSxhQUFhLEVBQUUsbUJBQW1CLENBQUM7aUJBQ3hGOztJQUNrQywyQkFBQztDQUFBLEFBZnBDLElBZW9DO1NBQXZCLG9CQUFvQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IE92ZXJsYXlNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jZGsvb3ZlcmxheSc7XG5pbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IEZvcm1zTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvZm9ybXMnO1xuXG5pbXBvcnQgeyBOekFkZE9uTW9kdWxlIH0gZnJvbSAnLi4vY29yZS9hZGRvbi9hZGRvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpOb0FuaW1hdGlvbk1vZHVsZSB9IGZyb20gJy4uL2NvcmUvbm8tYW5pbWF0aW9uL256LW5vLWFuaW1hdGlvbi5tb2R1bGUnO1xuXG5pbXBvcnQgeyBOekF1dG9jb21wbGV0ZU9wdGdyb3VwQ29tcG9uZW50IH0gZnJvbSAnLi9uei1hdXRvY29tcGxldGUtb3B0Z3JvdXAuY29tcG9uZW50JztcbmltcG9ydCB7IE56QXV0b2NvbXBsZXRlT3B0aW9uQ29tcG9uZW50IH0gZnJvbSAnLi9uei1hdXRvY29tcGxldGUtb3B0aW9uLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOekF1dG9jb21wbGV0ZVRyaWdnZXJEaXJlY3RpdmUgfSBmcm9tICcuL256LWF1dG9jb21wbGV0ZS10cmlnZ2VyLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBOekF1dG9jb21wbGV0ZUNvbXBvbmVudCB9IGZyb20gJy4vbnotYXV0b2NvbXBsZXRlLmNvbXBvbmVudCc7XG5cbkBOZ01vZHVsZSh7XG4gIGRlY2xhcmF0aW9uczogW1xuICAgIE56QXV0b2NvbXBsZXRlQ29tcG9uZW50LFxuICAgIE56QXV0b2NvbXBsZXRlT3B0aW9uQ29tcG9uZW50LFxuICAgIE56QXV0b2NvbXBsZXRlVHJpZ2dlckRpcmVjdGl2ZSxcbiAgICBOekF1dG9jb21wbGV0ZU9wdGdyb3VwQ29tcG9uZW50XG4gIF0sXG4gIGV4cG9ydHM6IFtcbiAgICBOekF1dG9jb21wbGV0ZUNvbXBvbmVudCxcbiAgICBOekF1dG9jb21wbGV0ZU9wdGlvbkNvbXBvbmVudCxcbiAgICBOekF1dG9jb21wbGV0ZVRyaWdnZXJEaXJlY3RpdmUsXG4gICAgTnpBdXRvY29tcGxldGVPcHRncm91cENvbXBvbmVudFxuICBdLFxuICBpbXBvcnRzOiBbQ29tbW9uTW9kdWxlLCBPdmVybGF5TW9kdWxlLCBGb3Jtc01vZHVsZSwgTnpBZGRPbk1vZHVsZSwgTnpOb0FuaW1hdGlvbk1vZHVsZV1cbn0pXG5leHBvcnQgY2xhc3MgTnpBdXRvY29tcGxldGVNb2R1bGUge31cbiJdfQ==